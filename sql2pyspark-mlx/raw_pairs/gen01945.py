from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .agg(
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
    )
)
