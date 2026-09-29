from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .groupBy("city", "is_active")
    .agg(
        F.count("*").alias("num_customers"),
        F.sum("age").alias("total_age"),
        F.avg("age").alias("avg_age"),
    )
    .orderBy(F.col("total_age").desc(), "num_customers")
)
