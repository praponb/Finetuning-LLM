from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") >= "2023-09-01") & F.col("monthly_fee").between(90, 150))
    .select(
        "id",
        "end_date",
        F.when(F.col("status") == "EXPIRED", 1).otherwise(0).alias("is_expired"),
    )
)
