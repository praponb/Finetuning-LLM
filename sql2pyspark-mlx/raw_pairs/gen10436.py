from pyspark.sql import functions as F

valid = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-03-28"))) & F.col("channel").isNotNull())
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
        F.when(F.col("amount") >= 1390, "High").when(F.col("amount") >= 390, "Medium").otherwise("Low").alias("amount_level"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

result = (
    valid
    .select(
        "id",
        "status",
        F.col("discount").alias("DISCOUNT"),
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
        "amount",
    )
    .unpivot(
        ids=["id", "status"],
        values=["DISCOUNT", "SHIPPING_FEE_VAL", "amount"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
