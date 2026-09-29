from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "PAUSED"]))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        (F.datediff("start_date", "end_date") / 7).alias("weeks_end_date_to_start_date"),
    )
)

result = (
    cleaned
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .rollup(F.col("c.country").alias("country"), F.col("e.plan").alias("plan"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("e.weeks_end_date_to_start_date")).alias("total_weeks_end_date_to_start_date"),
        F.grouping_id().alias("grouping_level"),
    )
)
