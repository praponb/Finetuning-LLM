from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 58, "HIGH").otherwise("LOW").alias("age_tier"),
        F.date_trunc("quarter", "signup_date").alias("quarter_start"),
    )
    .agg(
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
        F.avg(F.when(F.col("city") == "Berlin", F.col("credit_limit"))).alias("berlin_avg_credit_limit"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("credit_limit_per_bangkok"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.corr("age", "credit_limit").alias("corr_age_credit_limit_2"),
        F.sum(F.when(F.col("city") == "London", F.col("age")).otherwise(0)).alias("london_age"),
    )
)
