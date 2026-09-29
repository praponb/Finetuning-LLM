from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w2 = Window.partitionBy("order_id")
w3 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select(
        "*",
        F.to_date("paid_at").alias("paid_at_date"),
        F.round("amount", 2).alias("amount_rounded"),
    )
)

kept_keys = (
    valid
    .select("id")
    .intersectAll(
        df_payments
        .filter(F.hour("paid_at").between(11, 16))
        .select("id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.status").alias("order_status"))
)

metrics = (
    joined
    .withColumn("method_sum_amount", F.sum("amount").over(w))
    .withColumn("order_id_stddev_samp_amount_rounded", F.stddev_samp("amount_rounded").over(w2))
    .withColumn("amount_rounded_avg_30d", F.avg("amount_rounded").over(w3))
)

cleaned = (
    metrics
    .filter(F.col("paid_at") > F.to_timestamp(F.lit("2023-10-13 13:00:00")))
    .select(
        "*",
        F.initcap(F.trim("order_status")).alias("order_status_title"),
        F.coalesce("order_status", F.lit("UNKNOWN")).alias("resolved_order_status"),
    )
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.order_id",
        "src.method",
        "src.amount",
        "src.paid_at",
        "src.status",
        "src.paid_at_date",
        "src.amount_rounded",
        "src.order_status",
        "src.amount_rounded_avg_30d",
        "src.order_status_title",
        "src.resolved_order_status",
        F.col("o.channel"),
        F.col("o.amount").alias("order_amount"),
    )
)

result = (
    with_dims
    .groupBy("status")
    .pivot("channel", ["partner", "web"])
    .agg(F.sum("order_amount"))
)
