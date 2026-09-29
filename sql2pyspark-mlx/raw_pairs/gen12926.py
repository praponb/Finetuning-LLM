from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")

base = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("cost") <= 37))
    .withColumn("cost_rounded", F.round("cost", 1))
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

base_shipments = (
    latest
    .filter(F.col("carrier").isNotNull() & (F.col("carrier").eqNullSafe(F.lit("UPS"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled"),
    )
)

joined = (
    base_shipments
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.delivered_date",
        "src.days_shipped_date_to_delivered_date",
        "src.carrier_filled",
        F.col("b.channel"),
    )
)

result = (
    joined
    .select(
        "id",
        "order_id",
        "delivered_date",
        "carrier_filled",
        "channel",
        F.when(F.col("channel").rlike("^(web|store)$"), "GROUP_A").when(F.col("channel").rlike("^(mobile|partner)$"), "GROUP_B").otherwise("CORE").alias("channel_zone"),
    )
    .orderBy("id")
)
