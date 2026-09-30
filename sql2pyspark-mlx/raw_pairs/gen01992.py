from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") >= "2025-08-01") & (F.col("plan") == "premium") & (F.col("customer_id") == 30))
    .select(
        "id",
        "plan",
        "start_date",
        F.add_months("start_date", 1).alias("start_date_plus_months"),
    )
)
