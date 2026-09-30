from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 6, 8, 9]))
    .groupBy(
        F.when(F.col("quantity") >= 181, "high").when(F.col("quantity") >= 53, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") < 9)
    .select("quantity_bucket", "product_id", "quantity_count")
    .orderBy(F.col("product_id").desc(), "quantity_count")
)
