from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", "store_id")
    .agg(
        F.count("*").alias("total_count"),
        F.count("quantity").alias("quantity_count"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("total_count") > 3)
    .orderBy(F.col("total_count").desc(), F.col("quantity_count").desc())
)
