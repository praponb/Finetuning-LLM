from pyspark.sql import functions as F

quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

joined = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.store_id").isNotNull())
    .select("e.*", F.col("b.shipping_fee"), F.col("b.order_date"))
)

base = (
    joined
    .filter(F.col("shipped_date") > F.lit("2023-09-03").cast("date"))
    .withColumn("shipped_date_minus_89d", F.date_sub("shipped_date", 89))
    .withColumn("order_date_plus_months", F.add_months("order_date", 6))
)

matched_keys = (
    base
    .select("status", "carrier")
    .intersectAll(
        df_shipments
        .filter(F.col("delivered_date") <= "2024-06-01")
        .select("status", "carrier")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("carrier").isNotNull() & F.col("carrier").isin(["DHL", "FedEx", "Flash"]))
    .select(
        "*",
        (F.col("shipping_fee") * F.col("cost")).alias("shipping_fee_times_cost"),
        F.date_format("order_date_plus_months", "yyyy-MM-dd").alias("order_date_plus_months_label"),
    )
)

result = (
    filtered
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "status",
        "shipped_date_minus_89d",
        "order_date_plus_months",
        "shipping_fee_times_cost",
        F.when(F.col("status").rlike("^(DELIVERED|LOST)$"), "GROUP_A").when(F.col("status").rlike("^(IN_TRANSIT|RETURNED)$"), "CORE").otherwise("PRIORITY").alias("status_zone"),
        F.col("sum_order_item_quantity"),
    )
    .orderBy("cost", F.col("sum_order_item_quantity").desc())
)
