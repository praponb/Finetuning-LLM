from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id = Window.partitionBy("holder_id")
w_status_by_end_date_full = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_end_date_prior_3 = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id").rowsBetween(-3, -1)
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc_nulls_first(), "id")
w_holder_segment_by_start_date = Window.partitionBy("holder_segment").orderBy(F.col("start_date").desc(), "id")
w_holder_segment_by_start_date_2 = Window.partitionBy("holder_segment").orderBy(F.col("start_date").desc(), "id").rowsBetween(-3, Window.currentRow)

recent_policys = (
    df_policies
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

with_windows = (
    recent_policys
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
        F.size(F.collect_set("product").over(w_holder_id)).alias("distinct_product_in_holder_id"),
        F.last("end_date").over(w_status_by_end_date_full).alias("last_end_date"),
        F.max("premium").over(w_status_by_end_date_prior_3).alias("premium_max_3_prior"),
        F.round(F.percent_rank().over(w_holder_id_by_end_date), 3).alias("end_date_percent_rank"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"), F.count("*").alias("claim_count_2"))
)

summarized_rows = (
    with_windows.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.distinct_product_in_holder_id",
        "s.last_end_date",
        "s.premium_max_3_prior",
        "s.end_date_percent_rank",
        "s.claim_count",
        F.col("p.segment").alias("holder_segment"),
    )
)

with_trends = (
    combined_details
    .select(
        "*",
        F.row_number().over(w_holder_segment_by_start_date).alias("start_date_seq"),
        F.sum("end_date_percent_rank").over(w_holder_segment_by_start_date_2).alias("end_date_percent_rank_sum_4"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.ntile(4).over(w_holder_segment_by_start_date).alias("start_date_quartile"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "distinct_product_in_holder_id",
        "last_end_date",
        "premium_max_3_prior",
        "end_date_percent_rank",
        "claim_count",
        "holder_segment",
        "end_date_percent_rank_sum_4",
        "rows_in_holder_id",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        F.round(F.col("end_date_percent_rank") * 0.25, 2).alias("end_date_percent_rank_adj"),
    )
)
