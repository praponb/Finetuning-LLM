from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 237, "high").when(F.col("quantity") >= 90, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("max_quantity") > 180)
    .select("quantity_bucket", "max_quantity", "row_count")
)
