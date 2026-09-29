from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-04-01").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

remaining_keys = (
    base
    .select("store_id", "customer_id")
    .subtract(
        df_orders
        .filter(F.col("amount").between(510, 1480))
        .select("store_id", "customer_id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((F.col("channel").eqNullSafe(F.lit("mobile"))) & (F.col("days_since_order_date") > 30))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.length("channel").alias("channel_length"),
        (F.col("days_since_order_date") * F.col("discount")).alias("days_since_order_date_times_discount"),
    )
)

cleaned_orders = (
    cleaned
    .filter(F.col("days_since_order_date_times_discount").isNull())
    .select(
        "*",
        F.trim("status").alias("clean_status"),
        F.when(F.col("status").isin(["COMPLETED", "CANCELLED"]), "Group A").otherwise("Other").alias("status_group"),
    )
)

result = (
    cleaned_orders
    .groupBy("store_id")
    .pivot("status", ["RETURNED", "COMPLETED", "SHIPPED"])
    .agg(F.avg("shipping_fee"))
)
