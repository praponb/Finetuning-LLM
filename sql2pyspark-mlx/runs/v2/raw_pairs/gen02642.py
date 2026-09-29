from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter((F.col("channel") == "web") & F.col("customer_id").isNull() & F.col("shipping_fee").isNotNull())
)

agg = (
    valid_orders
    .groupBy(F.year("order_date").alias("order_year"), "channel")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_completed"),
        F.countDistinct("shipping_fee").alias("unique_customers"),
    )
)

result = (
    agg
    .select("order_year", "channel", "pct_completed", "unique_customers")
    .orderBy(F.col("channel").desc())
)
