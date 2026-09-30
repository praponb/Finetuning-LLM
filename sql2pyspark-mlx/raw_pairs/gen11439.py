from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("order_date").desc(), "id")
w3 = Window.partitionBy("status").orderBy("order_date")

cleaned = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-09-04 03:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.substring("method", 1, 4).alias("method_prefix"),
        F.coalesce("method", F.lit("NONE")).alias("resolved_method"),
    )
)

joined = (
    cleaned
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.amount") >= 820)
    .select(
        "e.id",
        "e.order_id",
        "e.paid_at",
        "e.status",
        "e.method_prefix",
        F.col("p.order_date"),
        F.col("p.channel"),
        F.col("p.shipping_fee"),
    )
)

deduped = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "status", "order_date", "channel", "shipping_fee")
)

latest = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "status", "order_date", "channel")
)

latest_payments = (
    latest
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

result = (
    latest_payments
    .groupBy("status")
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("status").desc())
)
