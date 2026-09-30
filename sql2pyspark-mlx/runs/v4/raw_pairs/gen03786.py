from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("order_order_date").desc_nulls_first(), "id")

valid = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-04-15").cast("date")) & (F.col("carrier") != "FedEx"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "status",
        F.date_add("shipped_date", 40).alias("shipped_date_plus_40d"),
        F.lower("status").alias("status_lower"),
    )
)

joined = (
    valid
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.shipping_fee"), F.col("b.amount"), F.col("b.order_date"))
)

joined_shipments = (
    joined
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.shipping_fee").alias("order_shipping_fee"),
        F.col("b.order_date").alias("order_order_date"),
        F.col("b.discount"),
    )
)

enriched = (
    joined_shipments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.*",
        F.col("d.shipping_fee").alias("order_shipping_fee_2"),
        F.col("d.order_date").alias("order_order_date_2"),
    )
)

enriched_shipments = (
    enriched
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.shipping_fee").alias("order_shipping_fee_3"),
        F.col("d.order_date").alias("order_order_date_3"),
        F.col("d.discount").alias("order_discount"),
    )
)

ranked = (
    enriched_shipments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .select(
        "id",
        "carrier",
        "shipped_date",
        "status",
        "status_lower",
        "shipping_fee",
        "amount",
        "order_date",
        "order_shipping_fee",
        F.when(F.col("carrier").rlike("^(Flash|FedEx)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("carrier_zone"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
