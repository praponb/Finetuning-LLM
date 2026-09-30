from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id")

base = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-09-07").cast("date")) & (F.col("last_restock") <= "2024-02-01"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_str"),
        F.floor("quantity").alias("quantity_whole"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft").between(8512, 15260))
    .select(
        "src.id",
        "src.store_id",
        "src.quantity",
        "src.last_restock",
        "src.quantity_str",
        F.col("s.store_name"),
    )
)

scored = (
    joined
    .select(
        "id",
        "last_restock",
        "quantity_str",
        "store_name",
        F.ntile(5).over(w).alias("quantity_bucket"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

valid = (
    scored
    .filter(F.upper("store_name").like("%IRP%"))
    .select(
        "id",
        "last_restock",
        "quantity_str",
        "quantity_bucket",
        "row_seq",
        F.ceil("row_seq").alias("row_seq_whole"),
    )
)

valid_inventory = (
    valid
    .filter(F.col("last_restock") > F.lit("2023-04-29").cast("date"))
    .select(
        "id",
        "last_restock",
        "quantity_str",
        "row_seq",
        "row_seq_whole",
        (F.col("quantity_bucket") * F.col("row_seq")).alias("quantity_bucket_times_row_seq"),
    )
)

result = (
    valid_inventory
    .select(
        "id",
        "last_restock",
        "quantity_str",
        "row_seq",
        "row_seq_whole",
        "quantity_bucket_times_row_seq",
    )
)
