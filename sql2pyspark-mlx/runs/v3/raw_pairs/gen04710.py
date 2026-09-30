from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "segment",
        F.when(F.col("credit_limit") >= 14500, "high").when(F.col("credit_limit") >= 3600, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.count("credit_limit").alias("credit_limit_count"),
        F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0)).alias("smb_count"),
    )
    .orderBy(F.col("smb_count").desc(), F.col("stddev_credit_limit").desc())
)
