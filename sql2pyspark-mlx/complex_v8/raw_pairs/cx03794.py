from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount").desc(), F.col("id").desc())
w_policy_product_policy_start = Window.partitionBy("policy_product", F.year("policy_start_date"))
w_all_by_claim_count_claim_typ = Window.orderBy(F.col("claim_count_claim_type_in_fire_theft").desc())
w_policy_status_policy_product = Window.partitionBy("policy_status", "policy_product").orderBy("policy_end_date_span_days", "policy_id", "policy_end_date_ym", "reported_at_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_claims = (
    df_claims
    .filter((F.col("approved_amount") < 23487.5) & (F.col("adjuster_id") >= 4))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
        F.last_day(F.col("claim_date")).alias("claim_date_month_end_2"),
    )
)

unique_rows = (
    base_claims
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

summarized_rows = (
    unique_rows.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

enriched_policy = (
    summarized_rows.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

totals = (
    enriched_policy
    .filter(F.col("adjuster_id") >= 3)
    .groupBy(
        "policy_id",
        F.date_format("policy_end_date", "yyyy-MM").alias("policy_end_date_ym"),
        F.date_format("reported_at", "yyyy-MM").alias("reported_at_ym"),
    )
    .agg(
        F.round(F.sum("claimed_amount"), 2).alias("total_claimed_amount"),
        F.datediff(F.max("policy_end_date"), F.min("policy_end_date")).alias("policy_end_date_span_days"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.sum(F.when(F.col("claim_type").isin("fire", "theft"), F.col("claim_count")).otherwise(0)).alias("claim_count_claim_type_in_fire_theft"),
    )
)

with_details = (
    totals.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.policy_id",
        "s.policy_end_date_ym",
        "s.reported_at_ym",
        "s.total_claimed_amount",
        "s.policy_end_date_span_days",
        "s.status_list",
        "s.claim_count_claim_type_in_fire_theft",
        F.col("p.status").alias("policy_status"),
        F.col("p.product").alias("policy_product"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

metrics = (
    with_details
    .select(
        "policy_id",
        "policy_end_date_ym",
        "reported_at_ym",
        "total_claimed_amount",
        "policy_end_date_span_days",
        "status_list",
        F.min("total_claimed_amount").over(w_policy_product_policy_start).alias("policy_product_policy_start_date_year_min_total_"),
        F.dense_rank().over(w_all_by_claim_count_claim_typ).alias("claim_count_claim_type_in_fire_theft_dense_rank"),
        F.last("policy_start_date", ignorenulls=True).over(w_policy_status_policy_product).alias("last_known_policy_start_date"),
    )
)

result = (
    metrics
    .cube("claim_count_claim_type_in_fire_theft_dense_rank", "policy_id")
    .agg(
        F.round(F.stddev("policy_end_date_span_days"), 2).alias("stddev_policy_end_date_span_days"),
        F.min("policy_end_date_span_days").alias("min_policy_end_date_span_days"),
    )
)
