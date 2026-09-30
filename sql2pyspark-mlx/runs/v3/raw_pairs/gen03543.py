from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("discount") >= 0.41, "high").when(F.col("discount") >= 0.1, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("discount_bucket").desc())
)
