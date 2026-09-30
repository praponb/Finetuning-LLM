from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~((F.col("customer_id") == 39) & (F.col("plan") == "premium") & (F.col("end_date") >= "2024-05-01")))
    .select(
        "id",
        "start_date",
        "status",
        F.date_trunc("WEEK", "start_date").alias("start_date_period"),
    )
    .orderBy("start_date", "id")
    .limit(10)
)
