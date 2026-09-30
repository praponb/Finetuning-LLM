from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("order_amount").desc_nulls_first(), "id")
w2 = Window.partitionBy("channel", "method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy(F.col("amount").desc(), "id")

filtered = (
    df_payments
    .filter(F.col("paid_at") >= "2024-09-27")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.upper("status").alias("status_upper"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.amount").alias("order_amount"), F.col("p.channel"))
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "method", "amount", "channel")
)

with_windows = (
    deduped
    .select(
        "*",
        F.max("amount").over(w2).alias("running_max_amount"),
        F.min("amount").over(w2).alias("running_min_amount"),
        F.avg("amount").over(w2).alias("running_avg_amount"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

result = (
    with_windows
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.min("running_min_amount").alias("min_running_min_amount"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.countDistinct("method").alias("distinct_methods"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("amount")).otherwise(0)).alias("promptpay_amount"),
    )
    .orderBy("min_running_min_amount")
)
