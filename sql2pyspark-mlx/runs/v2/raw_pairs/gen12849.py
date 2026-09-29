from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") != 159) | F.col("product_id").isin([5, 6]))
    .groupBy(
        F.when(F.col("quantity") >= 230, "high").when(F.col("quantity") >= 45, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("*").alias("num_inventory"))
    .filter(F.col("num_inventory") >= 2)
    .orderBy("quantity_bucket")
)
