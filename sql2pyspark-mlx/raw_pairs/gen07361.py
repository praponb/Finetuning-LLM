from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("order_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("method", "order_id").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("method").orderBy("order_amount", "id")
w4 = Window.orderBy(F.col("next_order_amount").asc_nulls_first())
w5 = Window.orderBy("amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("method")
w7 = Window.orderBy("next_order_amount_cume_dist", "id")

base = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.col("amount").cast("int").alias("amount_int"),
        F.date_format("paid_at", "MMM yyyy").alias("paid_at_label"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.method",
        "src.amount",
        "src.paid_at_label",
        "src.method_clean",
        F.col("b.order_date"),
        F.col("b.amount").alias("order_amount"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .filter(F.col("method") != "cash")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at_label",
        "method_clean",
        "latest_rank",
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.lead("order_amount", 2).over(w3).alias("next_order_amount"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "paid_at_label",
        "method_clean",
        "latest_rank",
        "amount_bucket",
        "next_order_amount",
        F.cume_dist().over(w4).alias("next_order_amount_cume_dist"),
        F.sum("amount_bucket").over(w5).alias("running_amount_bucket"),
        F.stddev_samp("next_order_amount").over(w6).alias("method_stddev_samp_next_order_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w6)), 4).alias("amount_share"),
    )
)

filtered = (
    with_windows
    .filter(F.col("next_order_amount_cume_dist").between(0.41, 0.64))
    .select(
        "id",
        "paid_at_label",
        "latest_rank",
        "amount_bucket",
        "next_order_amount",
        "next_order_amount_cume_dist",
        "running_amount_bucket",
        "method_stddev_samp_next_order_amount",
        (F.col("next_order_amount_cume_dist") - F.col("next_order_amount")).alias("next_order_amount_cume_dist_minus_next_order_amount"),
        F.when(F.col("running_amount_bucket") >= 40, "High").when(F.col("running_amount_bucket") >= 16, "Medium").otherwise("Low").alias("running_amount_bucket_band"),
    )
)

enriched_payments = (
    filtered
    .select(
        "id",
        "paid_at_label",
        "latest_rank",
        "next_order_amount",
        "method_stddev_samp_next_order_amount",
        "next_order_amount_cume_dist_minus_next_order_amount",
        F.lag("next_order_amount_cume_dist_minus_next_order_amount").over(w7).alias("prev_next_order_amount_cume_dist_minus_next_order_amount"),
        F.lead("amount_bucket").over(w7).alias("next_amount_bucket"),
        F.row_number().over(w7).alias("seq_no"),
    )
)

result = (
    enriched_payments
    .select(
        "id",
        "paid_at_label",
        "next_order_amount",
        "method_stddev_samp_next_order_amount",
        "next_amount_bucket",
        "seq_no",
    )
)
