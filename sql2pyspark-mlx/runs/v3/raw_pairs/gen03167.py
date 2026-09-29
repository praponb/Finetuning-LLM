from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("is_active") & F.col("city").isin(["Chiang Mai", "Berlin", "Singapore"])) | F.col("segment").isNull())
    .groupBy(
        "is_active",
        F.when(F.col("credit_limit") >= 13400, "high").when(F.col("credit_limit") >= 4000, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.max("signup_date").alias("latest_signup_date"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
)
