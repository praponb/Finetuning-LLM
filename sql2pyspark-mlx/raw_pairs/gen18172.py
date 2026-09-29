from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.concat_ws("-", "channel", "status").alias("channel_status_key"),
        (F.datediff(F.current_date(), "order_date") / 365.25).alias("years_since_order_date"),
    )
)

result = (
    valid
    .select("id", "status", "discount", "amount")
    .unpivot(
        ids=["id", "status"],
        values=["discount", "amount"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
