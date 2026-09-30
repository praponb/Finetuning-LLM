from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["TH", "DE", "US"]))
    .groupBy("is_active")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("age")).otherwise(0)).alias("singapore_age"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.sum("age").alias("total_age"),
    )
)
