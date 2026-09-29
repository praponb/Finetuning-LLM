from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("discount", "id")

with_dims = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.status",
        F.col("p.status").alias("order_status"),
        F.col("p.channel"),
        F.col("p.discount"),
    )
)

deduped = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "carrier", "order_status", "channel")
)

base = (
    deduped
    .filter((~F.col("carrier").eqNullSafe(F.lit("UPS"))) & F.col("order_status").isin(["SHIPPED", "RETURNED"]))
    .withColumn("channel_lower", F.lower("channel"))
    .withColumn("channel_lower_2", F.lower("channel"))
)

valid = (
    base
    .filter(F.col("order_status").isin(["CANCELLED", "SHIPPED"]))
    .select(
        "*",
        F.when(F.col("channel").isin(["mobile", "partner"]), "Group A").otherwise("Other").alias("channel_group"),
    )
)

filtered = (
    valid
    .filter(F.col("order_status").isNotNull())
    .select(
        "id",
        "carrier",
        "order_status",
        "channel",
        "channel_lower",
        "channel_lower_2",
        "channel_group",
        F.when(F.col("channel").isin(["web", "mobile"]), "Tier 1").otherwise("Other").alias("channel_group_2"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

result = (
    filtered
    .select(
        "id",
        "carrier",
        "channel_lower",
        "channel_lower_2",
        "channel_group",
        "channel_group_2",
        "channel_filled",
    )
    .orderBy(F.col("id").asc_nulls_last(), "carrier")
)
