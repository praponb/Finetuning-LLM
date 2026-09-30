from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-03-24"))) & (F.col("channel").eqNullSafe(F.lit("mobile"))))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        (F.col("discount") * F.col("amount")).alias("discount_times_amount"),
        F.substring("status", 1, 2).alias("status_prefix"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

result = (
    filtered
    .select("id", "customer_id", "amount", "discount")
    .unpivot(
        ids=["id", "customer_id"],
        values=["amount", "discount"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
