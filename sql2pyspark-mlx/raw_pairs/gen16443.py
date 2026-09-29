from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc(), "id")
w3 = Window.orderBy(F.col("last_restock").desc(), "id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

base = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-02-12")))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_num"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "quantity_num",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w).alias("row_seq_2"),
        F.row_number().over(w3).alias("seq_no_2"),
    )
)

matched_keys = (
    with_windows
    .select("id", "store_id")
    .subtract(
        df_inventory
        .filter(F.year("last_restock") >= 2024)
        .select("id", "store_id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

kept_keys = (
    reconciled
    .select("product_id")
    .subtract(
        df_inventory
        .filter(F.year("last_restock") >= 2024)
        .select("product_id")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "quantity_num",
        "seq_no",
        "row_seq",
        "seq_no_2",
        F.col("max_inventory_quantity"),
    )
)
