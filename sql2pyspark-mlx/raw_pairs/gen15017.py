from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-07-23").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

result = (
    cleaned
    .select("customer_id", "id")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date") < "2023-05-01")
        .select("customer_id", "id")
    )
)
