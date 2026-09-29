from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 110) & (F.col("plan") == "enterprise"))
)

subscriptions_stats = (
    src
    .groupBy(
        F.date_format("end_date", "yyyy-MM").alias("end_month"),
        F.year("start_date").alias("start_year"),
    )
    .agg(F.count("*").alias("total_count"))
)

result = subscriptions_stats
