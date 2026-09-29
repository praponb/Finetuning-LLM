from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date_plus_months").desc(), "id")
w2 = Window.orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy(F.col("shipping_fee").desc(), "id")
w4 = Window.orderBy(F.col("amount").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("carrier").orderBy(F.col("shipment_shipped_date").desc_nulls_first(), "id")
w7 = Window.partitionBy("carrier").orderBy("amount_rank", "id")

cleaned = (
    df_orders
    .filter(F.col("customer_id").isNull() & (F.col("order_date") > F.to_date(F.lit("2023-06-24"))))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        F.add_months("order_date", 6).alias("order_date_plus_months"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_date", "amount", "shipping_fee", "resolved_channel")
)

valid = (
    ranked
    .filter(F.col("order_date") > F.to_date(F.lit("2023-07-14")))
    .select(
        "id",
        "order_date",
        "amount",
        "shipping_fee",
        "resolved_channel",
        F.round("shipping_fee", 2).alias("shipping_fee_rounded"),
    )
)

with_windows = (
    valid
    .filter(F.col("order_date") < "2024-08-01")
    .select(
        "id",
        "resolved_channel",
        F.avg("shipping_fee").over(w2).alias("shipping_fee_avg_3"),
        F.ntile(5).over(w3).alias("shipping_fee_bucket"),
        F.rank().over(w4).alias("amount_rank"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.carrier"),
        F.col("c.shipped_date").alias("shipment_shipped_date"),
    )
)

latest = (
    with_latest
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "resolved_channel",
        "shipping_fee_avg_3",
        "carrier",
        "rank_in_group",
        F.when(F.col("carrier").rlike("^(DHL|FedEx)$"), "EXTENDED").when(F.col("carrier").rlike("^(Kerry|Flash)$"), "CORE").otherwise("GROUP_B").alias("carrier_zone"),
    )
    .orderBy(F.col("rank_in_group").asc_nulls_first())
)
