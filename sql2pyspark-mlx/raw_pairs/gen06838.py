from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "order_id").orderBy(F.col("paid_at").desc_nulls_last())
w4 = Window.partitionBy("order_id").orderBy("burst_id", "id")
w5 = Window.partitionBy("order_id").orderBy(F.col("discount").desc_nulls_last(), "id")

filtered = (
    df_payments
    .filter(~F.col("method").eqNullSafe(F.lit("card")))
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

latest = (
    sessions_payments
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    latest
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_dims = (
    ranked
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.store_id").isin([15, 20, 33, 34]))
    .select("s.id", "s.order_id", "s.new_group_flag", F.col("o.discount"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.new_group_flag",
        "a.discount",
        F.col("p.amount"),
        F.col("p.channel"),
    )
)

ranked_payments = (
    enriched
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked_payments
    .groupBy(
        F.when(F.col("amount") >= 1790, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.when(F.col("discount") >= 0.31, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
        "order_id",
    )
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("amount_per_web"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy("order_id")
)
