from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock_plus_months").desc(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("rn").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("last_restock_plus_months").asc_nulls_last(), "id")

filtered = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-05-20"))) & (F.col("store_id") == 1))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.when(F.col("quantity") >= 262, "High").when(F.col("quantity") >= 106, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

valid = (
    filtered
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-03-06"))) & F.col("store_id").isin([3, 4]))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.date_sub("last_restock", 26).alias("last_restock_minus_26d"),
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id", "last_restock_minus_26d", "last_restock_plus_months")
)

latest_inventory = (
    latest
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "store_id", "last_restock_minus_26d")
)

valid_inventory = (
    latest_inventory
    .filter(F.col("last_restock_minus_26d") > "2025-04-01")
    .select(
        "*",
        F.date_add("last_restock_minus_26d", 52).alias("last_restock_minus_26d_plus_52d"),
    )
)

result = valid_inventory.select("id", "store_id", "last_restock_minus_26d_plus_52d")
