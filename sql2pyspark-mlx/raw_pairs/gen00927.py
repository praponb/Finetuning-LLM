from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["premium", "enterprise", "standard"]))
    .select(
        "id",
        "plan",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
        (F.datediff(F.current_date(), "start_date") / 30).alias("months_since_start_date"),
    )
)
