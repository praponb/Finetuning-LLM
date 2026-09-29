from pyspark.sql import functions as F

filtered_customers = df_customers.filter((F.col("age") >= 46) | F.col("email").like("%@mail.co.th"))

credit_limit_bucket_summary = (
    filtered_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 15900, "high").when(F.col("credit_limit") >= 3000, "mid").otherwise("low").alias("credit_limit_bucket"),
        "country",
    )
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.sum(F.when(F.col("country") == "TH", F.col("credit_limit")).otherwise(0)).alias("th_credit_limit"),
    )
)

result = credit_limit_bucket_summary
