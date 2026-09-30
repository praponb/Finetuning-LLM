from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").asc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("method", "order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("amount", "id")
w5 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("amount").desc())
w_hist = Window.partitionBy("status").orderBy(F.col("prev_amount").asc_nulls_first())

ranked = (
    df_payments
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "method", "amount", "status")
)

metrics = (
    latest
    .withColumn("prev_amount", F.lag("amount").over(w4))
    .withColumn("next_amount", F.lead("amount").over(w4))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("last_known_method", F.last("method", ignorenulls=True).over(w5))
)

matched_keys = (
    metrics
    .select("status", "id")
    .subtract(
        df_payments
        .filter(~F.col("status").isin(["REFUNDED", "SUCCESS"]))
        .select("status", "id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "id",
        "method",
        "status",
        "prev_amount",
        "next_amount",
        F.rank().over(w6).alias("amount_rank"),
        F.percent_rank().over(w_hist).alias("prev_amount_percent_rank"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("amount_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("amount_rank_tier"),
        "method",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("amount_rank", 0.9).alias("amount_rank_p90"),
        F.percentile_approx("prev_amount_percent_rank", 0.5).alias("prev_amount_percent_rank_p50"),
        F.percentile_approx("next_amount", [0.25, 0.5, 0.75]).alias("next_amount_quartiles"),
    )
    .filter(F.col("event_count") > 1)
)
