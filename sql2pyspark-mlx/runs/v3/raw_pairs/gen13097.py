from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["premium", "standard"]) | F.col("end_date").between("2023-08-21", "2024-05-01"))
    .select(
        "id",
        "status",
        "monthly_fee",
        F.floor("monthly_fee").alias("monthly_fee_whole"),
    )
)
