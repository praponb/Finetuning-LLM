from pyspark.sql import functions as F

cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.concat("carrier", F.lit(" - "), "status").alias("carrier_status"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("resolved_carrier"),
    )
)

enriched = (
    cleaned
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount") < 640)
    .select("src.*", F.col("o.status").alias("order_status"), F.col("o.amount"))
)

valid = (
    enriched
    .filter((F.col("order_status").eqNullSafe(F.lit("CANCELLED"))) & (F.col("delivered_date") > F.lit("2023-09-20").cast("date")))
    .select(
        "id",
        "order_id",
        "cost",
        "amount",
        (F.col("cost") * F.col("amount")).alias("cost_times_amount"),
        F.coalesce("order_status", F.lit("UNKNOWN")).alias("resolved_order_status"),
    )
)

base = (
    valid
    .filter(F.col("cost_times_amount") >= 24)
    .select(
        "*",
        F.when(F.col("amount") >= 1310, "High").when(F.col("amount") >= 720, "Medium").otherwise("Low").alias("amount_band"),
    )
)

result = (
    base
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "cost",
        "amount",
        "resolved_order_status",
        "amount_band",
        F.col("min_shipment_cost"),
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
