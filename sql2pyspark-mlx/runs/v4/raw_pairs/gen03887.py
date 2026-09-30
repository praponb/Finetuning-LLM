from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w2 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.orderBy(F.col("amount_int").asc_nulls_last())
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("method", "status").orderBy(F.col("amount_int").desc_nulls_last())
w_ordered = Window.partitionBy("status").orderBy("amount")
w7 = Window.partitionBy("method")
w_hist = Window.partitionBy("status", "method").orderBy(F.col("amount_int_dense_rank").desc_nulls_first())
w_running = Window.partitionBy("method", "status")
w10 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("paid_at") >= "2024-04-11")
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("paid_at_label", F.date_format("paid_at", "yyyy-MM-dd"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("N/A")))
)

cleaned = (
    valid
    .filter(F.col("method").isNotNull() & F.col("amount_int").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        "amount_int",
        "paid_at_label",
        "resolved_method",
        F.upper("method").alias("method_upper"),
    )
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

kept_keys = (
    latest
    .select("method", "id")
    .subtract(
        df_payments
        .filter(F.col("method").isNull())
        .select("method", "id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .withColumn("amount_int_count_30d", F.count("amount_int").over(w2))
    .withColumn("amount_int_cume_dist", F.cume_dist().over(w3))
    .withColumn("status_stddev_samp_amount_int", F.stddev_samp("amount_int").over(w4))
    .withColumn("amount_int_dense_rank", F.dense_rank().over(w5))
)

enriched_payments = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w_ordered).alias("amount_cume_dist"),
        F.avg("amount").over(w7).alias("method_avg_amount"),
        F.rank().over(w_hist).alias("amount_int_dense_rank_rank"),
    )
)

scored = (
    enriched_payments
    .select(
        "*",
        F.max("amount_cume_dist").over(w4).alias("status_max_amount_cume_dist"),
        F.avg("amount").over(w_running).alias("method_avg_amount_2"),
        F.max("amount_int_cume_dist").over(w10).alias("amount_int_cume_dist_max_90d"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("status_max_amount_cume_dist") >= 1, "LARGE").otherwise("SMALL").alias("status_max_amount_cume_dist_tier"),
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount_int_count_30d")).otherwise(0)).alias("refunded_amount_int_count_30d"),
        F.percentile_approx("amount_int_cume_dist", [0.25, 0.5, 0.75]).alias("amount_int_cume_dist_quartiles"),
        F.round(F.sum("method_avg_amount") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("method_avg_amount_per_success"),
    )
)
