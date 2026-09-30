from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 23) & (F.month("end_date").isin([2, 3, 5]) | F.col("monthly_fee").between(110, 210)))
    .select(
        "plan",
        "status",
        "end_date",
        F.when(F.col("status").isin(["EXPIRED", "ACTIVE"]), "Core").otherwise("Other").alias("status_group"),
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
    )
)
