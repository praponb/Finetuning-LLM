from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") >= 80) & (F.col("plan") == "standard") & (F.col("status") == "PAUSED"))
    .select(
        "id",
        "status",
        "end_date",
        F.round((F.col("monthly_fee") * 1.1), 2).alias("monthly_fee_adjusted"),
    )
    .orderBy(F.col("status").desc())
)
