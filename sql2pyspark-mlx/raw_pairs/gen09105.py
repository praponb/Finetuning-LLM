from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method").orderBy("amount", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("running_max_amount").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy("running_min_amount_int", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("order_id")
w6 = Window.partitionBy("order_id", "method").orderBy("running_min_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("method").orderBy(F.col("seq_no").desc(), "id")
w8 = Window.orderBy(F.col("running_sum_running_max_amount_share").desc_nulls_last())
w9 = Window.orderBy("running_min_running_max_amount_share", "id")
w10 = Window.orderBy("row_seq", "id")
w11 = Window.orderBy("running_sum_running_max_amount_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("amount") >= 1410)
    .withColumn("status_method_key", F.concat_ws("/", "status", "method"))
)

enriched = (
    filtered
    .select(
        "*",
        F.min("amount").over(w).alias("running_min_amount"),
        F.avg("amount").over(w).alias("running_avg_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
    )
)

base = (
    enriched
    .filter((F.col("paid_at") > F.to_timestamp(F.lit("2023-06-22 00:00:00"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "running_min_amount",
        "running_avg_amount",
        "running_max_amount",
        F.col("running_min_amount").cast("int").alias("running_min_amount_int"),
        F.round((F.col("running_max_amount") * 1.07), 2).alias("adjusted_running_max_amount"),
        F.coalesce("method", F.lit("NONE")).alias("method_filled"),
    )
)

metrics = (
    base
    .filter(F.col("amount") > 1400)
    .select(
        "id",
        "order_id",
        "method",
        "running_min_amount",
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.row_number().over(w3).alias("seq_no"),
        F.avg("adjusted_running_max_amount").over(w4).alias("adjusted_running_max_amount_avg_6_prior"),
        F.round((F.col("running_max_amount") / F.sum("running_max_amount").over(w5)), 4).alias("running_max_amount_share"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "running_min_amount",
        "running_max_amount_share",
        F.avg("running_max_amount_share").over(w6).alias("running_avg_running_max_amount_share"),
        F.sum("running_max_amount_share").over(w6).alias("running_sum_running_max_amount_share"),
        F.min("running_max_amount_share").over(w6).alias("running_min_running_max_amount_share"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

scored_payments = (
    scored
    .select(
        "id",
        "running_sum_running_max_amount_share",
        "running_min_running_max_amount_share",
        F.percent_rank().over(w8).alias("running_sum_running_max_amount_share_percent_rank"),
        F.lead("running_min_running_max_amount_share", 1).over(w9).alias("next_running_min_running_max_amount_share"),
        F.ntile(5).over(w10).alias("row_seq_bucket"),
        F.sum("running_avg_running_max_amount_share").over(w11).alias("cumulative_running_avg_running_max_amount_share"),
    )
)

result = (
    scored_payments
    .groupBy(
        F.when(F.col("next_running_min_running_max_amount_share") >= 0.64, "HIGH").otherwise("LOW").alias("next_running_min_running_max_amount_share_tier"),
    )
    .agg(
        F.percentile_approx("running_sum_running_max_amount_share", [0.25, 0.5, 0.75]).alias("running_sum_running_max_amount_share_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("next_running_min_running_max_amount_share_tier").asc_nulls_last())
)
