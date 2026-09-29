from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.date_format("signup_date", "yyyy-MM").alias("signup_month"),
        F.when(F.col("credit_limit") >= 15100, "high").when(F.col("credit_limit") >= 7300, "mid").otherwise("low").alias("credit_limit_bucket"),
        "segment",
    )
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .filter(F.col("avg_credit_limit") < 62)
    .orderBy(
        F.col("segment").desc(),
        F.col("avg_credit_limit").desc(),
        "signup_month",
        "credit_limit_bucket",
    )
    .limit(1)
)
