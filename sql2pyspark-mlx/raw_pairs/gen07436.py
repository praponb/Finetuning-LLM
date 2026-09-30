from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@startup.io") & F.month("signup_date").isin([5, 7, 8]))
    .groupBy("segment")
    .agg(
        F.avg("age").alias("avg_age"),
        F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0)).alias("smb_count"),
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
    .filter(F.col("bangkok_count") >= 80)
    .orderBy(F.col("smb_count").desc())
)
