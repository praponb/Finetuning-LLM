from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 120) | (F.col("plan") == "basic"))
    .select(
        "id",
        "plan",
        "monthly_fee",
        "start_date",
        F.concat("status", F.lit("/"), "plan").alias("status_plan"),
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)
