from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & ~F.col("is_active"))
    .groupBy(
        F.when(F.col("credit_limit") >= 17400, "high").when(F.col("credit_limit") >= 2700, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.month("signup_date").alias("signup_month"),
        "segment",
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
)
