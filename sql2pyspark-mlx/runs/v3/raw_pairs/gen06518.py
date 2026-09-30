from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-05-01", "2025-03-11") & (F.col("start_date") >= "2025-03-01") & (F.col("customer_id") == 17))
    .select(
        "id",
        "start_date",
        "customer_id",
        "status",
        F.concat("plan", F.lit(" - "), "status").alias("plan_status"),
    )
)
