from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "start_date",
        "plan",
        F.round((F.datediff("start_date", "end_date") / 30), 1).alias("months_end_date_to_start_date"),
        F.round((F.col("monthly_fee") * 100), 2).alias("monthly_fee_scaled"),
    )
)
