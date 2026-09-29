from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), F.col("id").desc())
w_status_len_status = Window.partitionBy("status_len", "status")
w_status_by_premium = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id")
w_status_holder_id_by_end_date = Window.partitionBy("status", "holder_id").orderBy("end_date")
w_status_next_start_date_month = Window.partitionBy("status", F.month("next_start_date"))
w_status_len_distinct_holder_i = Window.partitionBy("status_len", "distinct_holder_id_in_status_len_status").orderBy("next_start_date", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_status_len_vs_status = Window.orderBy("status_len_vs_status_next_start_date_month_avg", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_status_len_vs_status_2 = Window.orderBy("status_len_vs_status_next_start_date_month_avg", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_policys = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-03-27").cast("date"), F.lit("2025-05-28").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

latest = (
    filtered_policys
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

sequenced = (
    latest
    .select(
        "id",
        "start_date",
        "status",
        "status_len",
        F.max("premium").over(w_status_len_status).alias("status_len_status_max_premium"),
        F.lead("start_date").over(w_status_by_premium).alias("next_start_date"),
        F.round(F.col("status_len") / F.nullif(F.sum("status_len").over(w_status_len_status), F.lit(0)), 4).alias("status_len_share_of_status_len_status"),
        F.dense_rank().over(w_status_holder_id_by_end_date).alias("end_date_dense_rank"),
        F.size(F.collect_set("holder_id").over(w_status_len_status)).alias("distinct_holder_id_in_status_len_status"),
    )
)

detail_rows_claims = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.start_date",
        "s.status",
        "s.status_len",
        "s.next_start_date",
        "s.status_len_share_of_status_len_status",
        "s.distinct_holder_id_in_status_len_status",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.status").alias("claim_status"),
    )
)

with_trends = (
    detail_rows_claims
    .select(
        "id",
        "start_date",
        "status",
        "status_len",
        "next_start_date",
        "distinct_holder_id_in_status_len_status",
        "claim_id",
        "claim_reported_at",
        "claim_adjuster_id",
        F.sum("status_len").over(w_status_next_start_date_month).alias("status_next_start_date_month_sum_status_len"),
        F.nth_value("next_start_date", 2).over(w_status_len_distinct_holder_i).alias("second_next_start_date"),
        F.round(F.col("status_len") - F.avg("status_len").over(w_status_next_start_date_month), 2).alias("status_len_vs_status_next_start_date_month_avg"),
        F.round(F.avg("status_len").over(w_status_next_start_date_month), 2).alias("status_next_start_date_month_avg_status_len"),
        F.round(F.col("status_len") / F.nullif(F.sum("status_len").over(w_status_next_start_date_month), F.lit(0)), 4).alias("status_len_share_of_status_next_start_date_month"),
    )
)

windowed = (
    with_trends
    .select(
        "id",
        "status",
        "status_len",
        "next_start_date",
        "distinct_holder_id_in_status_len_status",
        "claim_id",
        "claim_reported_at",
        "claim_adjuster_id",
        "second_next_start_date",
        "status_len_share_of_status_next_start_date_month",
        F.nth_value("second_next_start_date", 2).over(w_all_by_status_len_vs_status).alias("second_second_next_start_date"),
        F.min("status_len").over(w_all_by_status_len_vs_status_2).alias("min_status_len_so_far"),
    )
)

joined_claim = (
    windowed.alias("s")
    .join(df_claims.alias("c"), (F.col("c.id") == F.col("s.claim_id")) & (F.col("c.claim_date").between(F.lit("2024-06-07").cast("date"), F.lit("2025-11-12").cast("date"))), "inner")
    .filter(F.col("c.status").isin("PAID", "OPEN", "APPROVED"))
    .select(
        "s.id",
        "s.status",
        "s.status_len",
        "s.next_start_date",
        "s.distinct_holder_id_in_status_len_status",
        "s.claim_id",
        "s.claim_reported_at",
        "s.claim_adjuster_id",
        "s.second_next_start_date",
        "s.status_len_share_of_status_next_start_date_month",
        "s.second_second_next_start_date",
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id_2"),
    )
)

result = (
    joined_claim
    .select(
        "id",
        "status",
        "next_start_date",
        "distinct_holder_id_in_status_len_status",
        "claim_id",
        "claim_reported_at",
        "claim_adjuster_id",
        "second_next_start_date",
        "status_len_share_of_status_next_start_date_month",
        "second_second_next_start_date",
        "claim_policy_id",
        "claim_status",
    )
    .orderBy(
        F.col("status_len_share_of_status_next_start_date_month").desc_nulls_first(),
        "id",
        "claim_id",
    )
    .limit(5)
)
