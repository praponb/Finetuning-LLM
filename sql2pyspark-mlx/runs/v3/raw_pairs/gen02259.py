from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~((F.col("city") == "London") | ~F.col("is_active")))
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.round(((100 * F.sum(F.when(F.col("country") == "JP", 1).otherwise(0))) / F.count("*")), 1).alias("pct_jp"),
    )
)
