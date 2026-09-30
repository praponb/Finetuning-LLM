from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((~F.col("carrier").eqNullSafe(F.lit("FedEx"))) & F.col("carrier").isin(["DHL", "UPS"]))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.date_trunc("month", "delivered_date").alias("delivered_date_period"),
    )
)

joined = (
    cleaned
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.shipped_date",
        "a.cost",
        "a.delivered_date_period",
        F.col("p.channel"),
        F.col("p.amount"),
    )
)

filtered = (
    joined
    .filter(F.col("order_id").isNotNull())
    .withColumn("shipped_date_plus_months", F.add_months("shipped_date", 6))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

joined_shipments = (
    filtered
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id").eqNullSafe(F.col("o.id")), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.cost",
        "s.delivered_date_period",
        "s.channel",
        "s.amount",
        "s.shipped_date_plus_months",
        "s.resolved_channel",
        F.col("o.channel").alias("order_channel"),
    )
)

result = (
    joined_shipments
    .groupBy(
        F.date_trunc("quarter", "shipped_date_plus_months").alias("quarter_start"),
        F.when(F.col("amount") >= 1590, "LARGE").otherwise("SMALL").alias("amount_tier"),
        "order_channel",
    )
    .agg(
        F.min("delivered_date_period").alias("first_seen"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        ((F.unix_timestamp(F.max("delivered_date_period")) - F.unix_timestamp(F.min("delivered_date_period"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("delivered_date_period")) - F.unix_timestamp(F.min("delivered_date_period"))) / 60).alias("span_minutes_2"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_last())
)
