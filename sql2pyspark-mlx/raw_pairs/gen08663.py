from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("order_date") >= "2025-05-16")
    .withColumn("channel_group", F.when(F.col("channel").isin(["partner", "web"]), "Priority").otherwise("Other"))
    .withColumn("order_date_day", F.dayofmonth("order_date"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    valid
    .select("channel", "customer_id")
    .exceptAll(
        df_orders
        .filter(F.col("order_date") <= "2025-08-01")
        .select("channel", "customer_id")
    )
)
