from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-06-13").cast("date")) & F.col("customer_id").isin([21, 24, 30, 40]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.dayofmonth("order_date").alias("order_date_day"),
        F.substring("status", 1, 4).alias("status_prefix"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

result = (
    base
    .groupBy("customer_id", "store_id")
    .pivot("status", ["RETURNED", "SHIPPED", "COMPLETED"])
    .agg(F.avg("shipping_fee").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "customer_id",
        "store_id",
        F.col("RETURNED_value_sum").alias("returned_value_sum"),
        F.col("RETURNED_n").alias("returned_n"),
        F.col("SHIPPED_value_sum").alias("shipped_value_sum"),
        F.col("SHIPPED_n").alias("shipped_n"),
        F.col("COMPLETED_value_sum").alias("completed_value_sum"),
        F.col("COMPLETED_n").alias("completed_n"),
    )
)
