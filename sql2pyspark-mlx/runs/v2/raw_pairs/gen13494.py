from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") <= "2025-03-01") | F.col("status").isin(["ACTIVE", "EXPIRED"]))
    .select(
        "plan",
        "monthly_fee",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)
