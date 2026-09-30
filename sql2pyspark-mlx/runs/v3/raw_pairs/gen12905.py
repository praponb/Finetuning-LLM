from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("city").isin(["Berlin", "Singapore"]), "priority").otherwise("other").alias("city_group"),
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
    )
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") < 80)
    .select("city_group", "signup_month", "avg_credit_limit")
    .orderBy(F.col("city_group").desc())
)
