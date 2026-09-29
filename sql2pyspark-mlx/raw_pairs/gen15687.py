from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter(F.year("delivered_date") >= 2025)
    .select(
        "*",
        F.date_trunc("month", "delivered_date").alias("delivered_date_period"),
        F.col("cost").cast("int").alias("cost_int"),
    )
)

cleaned = (
    filtered
    .filter(F.col("shipped_date") >= F.lit("2023-08-06").cast("date"))
    .withColumn("is_fedex", F.when(F.col("carrier") == "FedEx", 1).otherwise(0))
    .withColumn("delivered_date_period_hour", F.hour("delivered_date_period"))
)

enriched = (
    cleaned
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.shipped_date",
        "src.status",
        "src.cost_int",
        "src.is_fedex",
        "src.delivered_date_period_hour",
        F.col("p.order_date"),
        F.col("p.discount"),
        F.col("p.shipping_fee"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.carrier",
        "e.status",
        "e.is_fedex",
        "e.delivered_date_period_hour",
        "e.order_date",
        "e.shipping_fee",
        F.col("p.amount"),
        F.col("p.discount").alias("order_discount"),
    )
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("delivered_date_period_hour") >= 78, "PREMIUM").otherwise("STANDARD").alias("delivered_date_period_hour_tier"),
    )
    .agg(
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_rows"),
        F.size(F.collect_set("carrier")).alias("num_carriers_2"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("order_discount", [0.25, 0.5, 0.75]).alias("order_discount_quartiles"),
    )
    .orderBy("fedex_rows")
)
