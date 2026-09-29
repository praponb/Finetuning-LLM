from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_status_by_greate = Window.partitionBy("adjuster_id", "status").orderBy("greatest_approved_amount_claimed_amount", "id")
w_status_by_claim_date = Window.partitionBy("status").orderBy(F.col("claim_date").desc(), "id")
w_status_by_claim_date_ties = Window.partitionBy("status").orderBy(F.col("claim_date").desc())
w_adjuster_id_status_by_greate_2 = Window.partitionBy("adjuster_id", "status").orderBy("greatest_approved_amount_claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_status_by_greate_3 = Window.partitionBy("adjuster_id", "status").orderBy("greatest_approved_amount_claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_by_greatest_appro = Window.partitionBy("claim_type").orderBy(F.col("greatest_approved_amount_claimed_amount").desc(), "id")
w_claim_type_last_claim_date_y = Window.partitionBy("claim_type", F.year("last_claim_date")).orderBy(F.col("last_claim_date").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_claim_type_last_claim_date_y_2 = Window.partitionBy("claim_type", F.year("last_claim_date")).orderBy(F.col("last_claim_date").asc_nulls_last(), "id")
w_claim_type_claim_date_decile = Window.partitionBy("claim_type", "claim_date_decile")
w_claim_date_month_status_by_d = Window.partitionBy(F.date_trunc("month", "claim_date"), "status").orderBy(F.col("days_since_prev_claim_date").desc(), "id").rowsBetween(-1, -1)
w_last_claim_date_year_adjuste = Window.partitionBy(F.year("last_claim_date"), "adjuster_id").orderBy("last_claim_date", "id")
w_claim_date_year_cnt = Window.partitionBy("claim_date_year", "cnt")
w_all_by_claim_date_span_days = Window.orderBy(F.col("claim_date_span_days").desc())

valid_claims = (
    df_claims
    .filter((F.col("claimed_amount") > 11235.6) & (F.col("adjuster_id") <= 6))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("status_ref"),
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

metrics = (
    valid_claims
    .select(
        "*",
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_adjuster_id_status_by_greate)).alias("days_since_prev_claim_date"),
        F.ntile(10).over(w_status_by_claim_date).alias("claim_date_decile"),
        F.round(F.cume_dist().over(w_status_by_claim_date_ties), 3).alias("claim_date_cume_dist"),
        F.row_number().over(w_status_by_claim_date).alias("seq_no"),
        F.last("claim_date", ignorenulls=True).over(w_adjuster_id_status_by_greate_2).alias("last_known_claim_date"),
        F.last("claim_date").over(w_adjuster_id_status_by_greate_3).alias("last_claim_date"),
        F.round(F.cume_dist().over(w_claim_type_by_greatest_appro), 3).alias("greatest_approved_amount_claimed_amount_cume_dis"),
    )
)

top_rows = (
    metrics
    .filter(F.col("seq_no") <= 3)
    .drop("seq_no")
)

with_windows = (
    top_rows
    .select(
        "*",
        F.round(F.avg("days_since_prev_claim_date").over(w_claim_type_last_claim_date_y), 2).alias("days_since_prev_claim_date_avg_4"),
        F.rank().over(w_claim_type_last_claim_date_y_2).alias("last_claim_date_rank"),
        F.round(F.col("claim_date_decile") - F.avg("claim_date_decile").over(w_claim_type_claim_date_decile), 2).alias("claim_date_decile_vs_claim_type_claim_date_decil"),
        F.max("claim_date_cume_dist").over(w_claim_date_month_status_by_d).alias("claim_date_cume_dist_max_1_prior"),
        F.round(F.avg("claim_date_decile").over(w_claim_date_month_status_by_d), 2).alias("claim_date_decile_avg_1_prior"),
        F.dense_rank().over(w_last_claim_date_year_adjuste).alias("last_claim_date_dense_rank"),
        F.round(F.col("claim_date_cume_dist") / F.nullif(F.sum("claim_date_cume_dist").over(w_claim_type_claim_date_decile), F.lit(0)), 4).alias("claim_date_cume_dist_share_of_claim_type_claim_d"),
        F.size(F.collect_set("policy_id").over(w_claim_type_claim_date_decile)).alias("distinct_policy_id_in_claim_type_claim_date_deci"),
    )
)

per_group = (
    with_windows
    .groupBy("claim_date_decile", F.year("claim_date").alias("claim_date_year"))
    .agg(
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.count("*").alias("cnt"),
        F.avg("claim_date_decile").alias("avg_claim_date_decile_check"),
    )
    .filter(F.col("avg_claim_date_decile_check") > 4)
    .drop("avg_claim_date_decile_check")
)

ranked = (
    per_group
    .select(
        "claim_date_decile",
        "claim_date_year",
        "claim_date_span_days",
        "cnt",
        F.round(F.avg("claim_date_decile").over(w_claim_date_year_cnt), 2).alias("claim_date_year_cnt_avg_claim_date_decile"),
        F.count(F.lit(1)).over(w_claim_date_year_cnt).alias("rows_in_claim_date_year_cnt"),
        F.round(F.col("claim_date_span_days") / F.nullif(F.sum("claim_date_span_days").over(w_claim_date_year_cnt), F.lit(0)), 4).alias("claim_date_span_days_share_of_claim_date_year_cn"),
        F.round(F.cume_dist().over(w_all_by_claim_date_span_days), 3).alias("claim_date_span_days_cume_dist"),
    )
)

result = (
    ranked
    .select(
        "claim_date_decile",
        "claim_date_year",
        "cnt",
        "claim_date_year_cnt_avg_claim_date_decile",
        "rows_in_claim_date_year_cnt",
        "claim_date_span_days_cume_dist",
        (F.col("claim_date_span_days") + F.col("claim_date_decile")).alias("claim_date_span_days_plus_claim_date_decile"),
        F.coalesce(F.col("claim_date_year"), F.lit(0)).alias("claim_date_year_filled"),
    )
)
