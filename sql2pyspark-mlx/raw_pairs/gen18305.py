from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("discount").desc(), "id")
w3 = Window.partitionBy("channel").orderBy(F.col("running_min_amount").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_first())

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-06-23"))) & (~F.col("method").eqNullSafe(F.lit("paypal"))))
    .select(
        "*",
        F.concat_ws(",", "status", "method").alias("status_method_key"),
        F.round((F.col("amount") * 1.07), 2).alias("adjusted_amount"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.method",
        "src.amount",
        "src.status",
        "src.status_method_key",
        F.col("b.discount"),
        F.col("b.order_date"),
        F.col("b.status").alias("order_status"),
    )
)

enriched = (
    joined
    .filter(F.col("method") == "bank_transfer")
    .withColumn("running_sum_amount", F.sum("amount").over(w_hist))
    .withColumn("running_min_amount", F.min("amount").over(w_hist))
    .withColumn("running_max_amount", F.max("amount").over(w_hist))
    .withColumn("running_count_amount", F.count("amount").over(w_hist))
    .withColumn("event_seq", F.row_number().over(w2))
)

enriched_payments = (
    enriched
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select(
        "src.*",
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("o.amount").alias("order_amount"),
    )
)

result = (
    enriched_payments
    .select(
        "id",
        "method",
        "amount",
        "status_method_key",
        "order_status",
        "running_min_amount",
        "running_max_amount",
        "running_count_amount",
        "channel",
        F.row_number().over(w3).alias("event_seq_2"),
        F.dense_rank().over(w4).alias("amount_dense_rank"),
    )
)
