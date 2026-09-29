from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_product_or_unknown = Window.partitionBy("holder_id", "product_or_unknown").orderBy(F.col("start_date").desc_nulls_first(), "id", "claim_id")
w_holder_id_by_start_date_full = Window.partitionBy("holder_id").orderBy("start_date", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_status_by_claim_report = Window.partitionBy("claim_status").orderBy(F.col("claim_reported_at").desc(), "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_by_start_date_ties = Window.partitionBy("holder_id").orderBy("start_date")
w_holder_id_product_or_unknown_2 = Window.partitionBy("holder_id", "product_or_unknown").orderBy(F.col("start_date").desc_nulls_first(), "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_seq_no_by_second_start_date = Window.partitionBy("seq_no").orderBy(F.col("second_start_date").asc_nulls_last(), "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_reported_at = Window.orderBy("claim_reported_at", "id", "claim_id")
w_claim_type = Window.partitionBy("claim_type")
w_all_by_claim_reported_at_mov = Window.orderBy("claim_reported_at", "id", "claim_id").rowsBetween(-1, Window.currentRow)
w_claim_id_by_seq_no_prior_6 = Window.partitionBy("claim_id").orderBy(F.col("seq_no").asc_nulls_last(), "id", "claim_id").rowsBetween(-6, -1)
w_claim_id_by_seq_no = Window.partitionBy("claim_id").orderBy(F.col("seq_no").asc_nulls_last(), "id", "claim_id")
w_all_by_start_date_percent_ra = Window.orderBy("start_date_percent_rank_share_of_claim_type", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date_percent_ra_2 = Window.orderBy("start_date_percent_rank_share_of_claim_type", "id", "claim_id")
w_claim_type_second_start_date = Window.partitionBy("claim_type", F.date_trunc("month", "second_start_date"))

valid_policys = (
    df_policies
    .filter((F.col("start_date").between(F.lit("2024-07-19").cast("date"), F.lit("2025-03-24").cast("date"))) | (F.col("premium") >= 2013.8))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.abs(F.col("premium") - 3465.4).alias("premium_distance"),
    )
)

with_children = (
    valid_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.status",
        "s.product_or_unknown",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.status").alias("claim_status"),
        "c.claim_date",
    )
)

combined_details_claim = (
    with_children.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.product_or_unknown",
        "s.claim_id",
        "s.claim_type",
        "s.claim_status",
        F.col("c.status").alias("claim_status_2"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

metrics = (
    combined_details_claim
    .select(
        "*",
        F.row_number().over(w_holder_id_product_or_unknown).alias("seq_no"),
        F.first("claim_reported_at").over(w_holder_id_by_start_date_full).alias("first_claim_reported_at"),
        F.last("start_date").over(w_claim_status_by_claim_report).alias("last_start_date"),
        F.round(F.percent_rank().over(w_holder_id_by_start_date_ties), 3).alias("start_date_percent_rank"),
        F.nth_value("start_date", 2).over(w_holder_id_product_or_unknown_2).alias("second_start_date"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.first("seq_no").over(w_seq_no_by_second_start_date).alias("first_seq_no"),
        F.row_number().over(w_all_by_claim_reported_at).alias("claim_reported_at_seq"),
        F.round(F.col("seq_no") / F.nullif(F.sum("seq_no").over(w_claim_type), F.lit(0)), 4).alias("seq_no_share_of_claim_type"),
        F.round(F.col("start_date_percent_rank") / F.nullif(F.sum("start_date_percent_rank").over(w_claim_type), F.lit(0)), 4).alias("start_date_percent_rank_share_of_claim_type"),
        F.round(F.avg("seq_no").over(w_all_by_claim_reported_at_mov), 2).alias("seq_no_avg_2"),
    )
)

combined_details_claim_2 = (
    windowed.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.product_or_unknown",
        "s.claim_id",
        "s.claim_type",
        "s.claim_status",
        "s.claim_reported_at",
        "s.seq_no",
        "s.start_date_percent_rank",
        "s.second_start_date",
        "s.first_seq_no",
        "s.claim_reported_at_seq",
        "s.seq_no_share_of_claim_type",
        "s.start_date_percent_rank_share_of_claim_type",
        "s.seq_no_avg_2",
        F.col("c.claim_type").alias("claim_claim_type"),
        "c.claim_date",
        F.col("c.reported_at").alias("claim_reported_at_2"),
    )
)

windowed_2 = (
    combined_details_claim_2
    .select(
        "id",
        "product",
        "start_date",
        "claim_id",
        "claim_type",
        "claim_reported_at",
        "seq_no",
        "start_date_percent_rank",
        "claim_reported_at_seq",
        "start_date_percent_rank_share_of_claim_type",
        "seq_no_avg_2",
        "claim_claim_type",
        "claim_date",
        "claim_reported_at_2",
        F.sum("seq_no_share_of_claim_type").over(w_claim_id_by_seq_no_prior_6).alias("seq_no_share_of_claim_type_sum_6_prior"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_claim_id_by_seq_no)).alias("days_since_prev_start_date"),
        F.last("second_start_date", ignorenulls=True).over(w_all_by_start_date_percent_ra).alias("last_known_second_start_date"),
        F.lead("seq_no_avg_2").over(w_claim_id_by_seq_no).alias("next_seq_no_avg_2"),
        F.round(F.cume_dist().over(w_all_by_start_date_percent_ra_2), 3).alias("start_date_percent_rank_share_of_claim_type_cume"),
        F.size(F.collect_set("claim_id").over(w_claim_type_second_start_date)).alias("distinct_claim_id_in_claim_type_second_start_dat"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "claim_id",
        "claim_type",
        "seq_no",
        "claim_reported_at_seq",
        "start_date_percent_rank_share_of_claim_type",
        "seq_no_avg_2",
        "claim_date",
        "claim_reported_at_2",
        "days_since_prev_start_date",
        "last_known_second_start_date",
        "next_seq_no_avg_2",
        F.when(F.col("claim_claim_type").isin("flood", "delay"), "group_a").when(F.col("claim_claim_type") == "collision", "group_b").otherwise("other").alias("claim_claim_type_group"),
        F.month(F.col("claim_reported_at")).alias("claim_reported_at_month"),
    )
    .orderBy(
        F.col("claim_reported_at_seq").asc(),
        F.col("last_known_second_start_date").desc(),
    )
)
