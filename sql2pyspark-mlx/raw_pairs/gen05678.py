from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("shipment_status").orderBy(F.col("order_date").desc(), "id")

deduped = (
    df_orders
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.amount",
        "s.shipping_fee",
        "s.dedup_rank",
        F.col("c.carrier"),
        F.col("c.status").alias("shipment_status"),
    )
)

deduped_orders = (
    latest_match
    .withColumn("dedup_rank_2", F.row_number().over(w3))
    .filter(F.col("dedup_rank_2") == 1)
)

valid = (
    deduped_orders
    .filter((F.col("order_date") > F.lit("2023-07-19").cast("date")) & F.col("shipment_status").isNotNull())
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        "dedup_rank",
        "carrier",
        F.coalesce("shipping_fee", F.lit(0)).alias("shipping_fee_filled"),
        F.when(F.col("carrier").isin(["Kerry", "UPS"]), "Group A").otherwise("Other").alias("carrier_group"),
        F.coalesce("shipment_status", F.lit("UNKNOWN")).alias("resolved_shipment_status"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("shipping_fee") >= 30, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
    )
    .orderBy(F.col("shipping_fee_tier").asc_nulls_first())
)
