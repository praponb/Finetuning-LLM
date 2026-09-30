from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2024-02-26"))) & F.col("store_id").isin([1, 3, 4, 5]))
    .select(
        "id",
        "store_id",
        "product_id",
        F.col("quantity").cast("string").alias("quantity_str"),
    )
)

result = (
    valid
    .groupBy("store_id")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .orderBy(F.col("store_id").desc())
)
