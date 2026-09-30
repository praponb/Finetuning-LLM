from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") != "standard") & (F.col("start_date") >= "2023-11-01"))
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "start_date",
        (F.datediff(F.current_date(), "end_date") / 30).alias("months_since_end_date"),
    )
    .orderBy(F.col("months_since_end_date").desc())
)
