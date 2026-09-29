from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("months_since_start_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id").orderBy("months_since_start_date", "id")
w3 = Window.partitionBy("customer_id").orderBy("rank_in_group", "id")
w4 = Window.partitionBy("customer_id").orderBy("months_since_start_date", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("customer_id")
w6 = Window.partitionBy(F.year("end_date"), "customer_id").orderBy(F.col("monthly_fee").asc_nulls_first())
w7 = Window.orderBy("monthly_fee_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w_running = Window.orderBy("monthly_fee_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_subscriptions
    .filter(F.col("end_date") >= F.lit("2023-06-04").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        (F.datediff(F.current_date(), "start_date") / 30).alias("months_since_start_date"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

valid = (
    deduped
    .filter(F.col("end_date") > F.lit("2023-09-19").cast("date"))
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "months_since_start_date",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
)

deduped_subscriptions = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped_subscriptions
    .filter(F.col("customer_id") == 2)
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "months_since_start_date",
        "rank_in_group",
        F.lag("monthly_fee_whole", 1).over(w3).alias("prev_monthly_fee_whole"),
        F.avg("monthly_fee").over(w4).alias("monthly_fee_avg_5"),
        F.max("monthly_fee_whole").over(w5).alias("customer_id_max_monthly_fee_whole"),
        F.dense_rank().over(w6).alias("monthly_fee_dense_rank"),
        F.sum("monthly_fee").over(w7).alias("cumulative_monthly_fee"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "customer_id",
        "prev_monthly_fee_whole",
        "monthly_fee_avg_5",
        "monthly_fee_dense_rank",
        F.lead("customer_id_max_monthly_fee_whole").over(w_ordered).alias("next_customer_id_max_monthly_fee_whole"),
        F.lead("monthly_fee_avg_5").over(w_ordered).alias("next_monthly_fee_avg_5"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.last("monthly_fee_dense_rank", ignorenulls=True).over(w_running).alias("last_known_monthly_fee_dense_rank"),
    )
)

joined = (
    with_windows
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.segment"), F.col("d.signup_date"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("last_known_monthly_fee_dense_rank") >= 7, "HIGH").otherwise("LOW").alias("last_known_monthly_fee_dense_rank_tier"),
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.percentile_approx("last_known_monthly_fee_dense_rank", 0.5).alias("last_known_monthly_fee_dense_rank_p50"),
    )
)
