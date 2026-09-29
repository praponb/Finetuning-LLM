from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-07-21").cast("date")) & F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
    )
)

result = (
    base
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["city", "country"], ["city"], []],
        F.col("c.city").alias("city"),
        F.col("c.country").alias("country"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("e.monthly_fee_filled")), 2).alias("total_monthly_fee_filled"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("total_monthly_fee_filled").desc())
)
