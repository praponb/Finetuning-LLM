from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-06-13").cast("date")) & (F.col("delivered_date") <= "2023-10-27"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("cost") >= 78, "High").when(F.col("cost") >= 32, "Medium").otherwise("Low").alias("cost_band"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled"),
    )
)

kept_keys = (
    filtered
    .select("status", "carrier")
    .subtract(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("status", "carrier")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("shipped_date") > F.lit("2023-06-21").cast("date"))
    .withColumn("delivered_date_plus_months", F.add_months("delivered_date", 6))
)

enriched = (
    base
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.status") == "PENDING")
    .select(
        "src.*",
        F.col("b.shipping_fee"),
        F.col("b.status").alias("order_status"),
        F.col("b.amount"),
    )
)

base_shipments = (
    enriched
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.round((F.col("shipping_fee") / F.col("cost")), 2).alias("shipping_fee_per_cost"),
    )
)

enriched_shipments = (
    base_shipments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select(
        "s.*",
        F.col("o.status").alias("order_status_2"),
        F.col("o.discount"),
        F.col("o.channel"),
    )
)

joined = (
    enriched_shipments
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.status").alias("order_status_3"),
        F.col("b.amount").alias("order_amount"),
        F.col("b.shipping_fee").alias("order_shipping_fee"),
    )
)

result = (
    joined
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .groupBy(
        F.col("d.channel").alias("order_channel"),
        F.col("d.status").alias("order_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("src.discount")).alias("total_discount"),
        F.min(F.col("src.shipping_fee_per_cost")).alias("min_shipping_fee_per_cost"),
    )
    .orderBy(F.col("order_channel").asc_nulls_first())
)
