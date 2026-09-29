from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "enterprise") & ((F.col("monthly_fee") > 120) | F.col("customer_id").isNull()))
    .select(
        "id",
        "plan",
        "status",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
    .orderBy("plan", F.col("id").desc())
)
