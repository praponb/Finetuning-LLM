from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy(F.to_date("paid_at"), "method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method", "status").orderBy("amount_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("method").isNotNull() & (F.col("paid_at") > F.lit("2023-06-27").cast("date")))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("resolved_method", F.coalesce("method", F.lit("DIRECT")))
)

enriched = (
    valid
    .filter(F.col("amount_filled") < 42)
    .select(
        "*",
        F.count("amount").over(w).alias("last3_count_amount"),
        F.min("amount").over(w).alias("last3_min_amount"),
        F.sum("amount").over(w).alias("last3_sum_amount"),
        F.sum("amount_filled").over(w2).alias("running_amount_filled"),
    )
)

cleaned = (
    enriched
    .filter(F.col("last3_sum_amount").isNotNull() & F.col("method").isNotNull())
    .withColumn("method_prefix", F.substring("method", 1, 4))
    .withColumn("resolved_method_2", F.coalesce("method", F.lit("UNKNOWN")))
)

gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w3) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w4))

metrics = (
    sessions_payments
    .select(
        "*",
        F.max("is_new_burst").over(w4).alias("running_max_is_new_burst"),
        F.count("is_new_burst").over(w4).alias("running_count_is_new_burst"),
        F.sum("is_new_burst").over(w4).alias("running_sum_is_new_burst"),
        F.min("is_new_burst").over(w4).alias("running_min_is_new_burst"),
        F.last("amount_filled", ignorenulls=True).over(w5).alias("last_known_amount_filled"),
    )
)

result = (
    metrics
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .withColumn("last3_count_amount_tier", F.when(F.col("last3_count_amount") >= 18, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["year_start", "last3_count_amount_tier"], ["year_start"]],
        "year_start",
        "last3_count_amount_tier",
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.size(F.collect_set("method")).alias("num_methods_2"),
        F.percentile_approx("running_count_is_new_burst", 0.95).alias("running_count_is_new_burst_p95"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("num_methods").desc_nulls_first())
)
