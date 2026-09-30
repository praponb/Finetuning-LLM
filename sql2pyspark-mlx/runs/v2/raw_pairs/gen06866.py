from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 212, "high").when(F.col("quantity") >= 74, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
    .orderBy("quantity_bucket")
)
