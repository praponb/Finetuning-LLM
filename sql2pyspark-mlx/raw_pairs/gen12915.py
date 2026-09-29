from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-08-16").cast("date"))
    .select("*", F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"))
)

metrics = (
    base
    .withColumn("running_max_amount", F.max("amount").over(w_running))
    .withColumn("running_min_amount", F.min("amount").over(w_running))
    .withColumn("running_sum_amount", F.sum("amount").over(w_running))
)

enriched = (
    metrics
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.status").alias("order_status"))
)

matched_keys = (
    enriched
    .select("id", "status")
    .intersectAll(
        df_payments
        .filter(F.col("method") == "cash")
        .select("id", "status")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("running_sum_amount_tier", F.when(F.col("running_sum_amount") >= 8300, "HIGH").otherwise("LOW"))
    .withColumn("week_start", F.date_trunc("WEEK", "paid_at"))
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .cube("running_sum_amount_tier", "week_start", "year_start")
    .agg(
        F.percentile_approx("running_sum_amount", 0.95).alias("running_sum_amount_p95"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_rows"),
        F.max("amount").alias("max_amount"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_count"),
        F.min("running_sum_amount").alias("min_running_sum_amount"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("min_running_sum_amount") > 0)
    .orderBy(F.col("running_sum_amount_tier").asc_nulls_first())
)
