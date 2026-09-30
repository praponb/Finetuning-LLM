from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2024-03-03")
    .select(
        "id",
        "plan",
        F.when(F.col("status") == "CANCELLED", 1).otherwise(0).alias("is_cancelled"),
        F.date_sub("end_date", 10).alias("end_date_minus_10d"),
    )
    .orderBy(F.col("is_cancelled").desc())
)
