from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("discount").asc_nulls_last())
w2 = Window.partitionBy("channel").orderBy(F.col("discount").asc_nulls_first())
w3 = Window.partitionBy("status", "channel").orderBy("cost")

with_dims = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.amount"), F.col("d.channel"), F.col("d.discount"))
)

matched_keys = (
    with_dims
    .select("status")
    .exceptAll(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("status")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .withColumn("discount_dense_rank", F.dense_rank().over(w))
    .withColumn("discount_rank", F.rank().over(w2))
    .withColumn("cost_rank", F.rank().over(w3))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("discount") >= 0.31, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
        "carrier",
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost_2"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("cost_per_mobile"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("discount_rank")).otherwise(0)).alias("delivered_discount_rank"),
        F.count_if(F.col("status") == "IN_TRANSIT").alias("in_transit_events"),
    )
    .orderBy(F.col("avg_cost").desc())
)
