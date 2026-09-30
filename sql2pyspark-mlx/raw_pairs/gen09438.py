from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("end_date") > F.to_date(F.lit("2023-08-16")))
    .withColumn("monthly_fee_int", F.col("monthly_fee").cast("int"))
)

result = (
    base
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["city", "country", "plan"], ["city"], []],
        F.col("p.city").alias("city"),
        F.col("p.country").alias("country"),
        F.col("src.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("src.monthly_fee")), 2).alias("avg_monthly_fee"),
        F.round(F.avg(F.col("src.monthly_fee_int")), 2).alias("avg_monthly_fee_int"),
        F.grouping_id().alias("gid"),
    )
)
