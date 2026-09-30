from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.min("quantity").alias("min_inventory_quantity"))

with_windows = (
    df_inventory
    .withColumn("next_quantity", F.lead("quantity", 1, 0).over(w))
    .withColumn("store_id_max_quantity", F.max("quantity").over(w2))
)

base = (
    with_windows
    .filter(F.col("last_restock") <= "2025-08-24")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "next_quantity",
        "store_id_max_quantity",
        F.date_trunc("YEAR", "last_restock").alias("last_restock_period"),
    )
)

remaining_keys = (
    base
    .select("product_id", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("quantity") > 149)
        .select("product_id", "store_id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("quantity") <= 162)
    .withColumn("store_id_max_quantity_times_quantity", (F.col("store_id_max_quantity") * F.col("quantity")))
)

result = (
    cleaned
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "last_restock",
        "next_quantity",
        "store_id_max_quantity",
        "last_restock_period",
        F.col("min_inventory_quantity"),
    )
    .orderBy(F.col("min_inventory_quantity").asc_nulls_first())
)
