from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_year_by_end_date_ti = Window.partitionBy(F.year("end_date")).orderBy(F.col("end_date").desc())
w_end_date_year_by_end_date_fu = Window.partitionBy(F.year("end_date")).orderBy(F.col("end_date").desc(), "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_id_holder_id_by_end_da = Window.partitionBy("claim_id", "holder_id").orderBy(F.col("end_date").desc())
w_claim_id_holder_id_by_end_da_2 = Window.partitionBy("claim_id", "holder_id").orderBy(F.col("end_date").desc(), "id", "claim_id")

recent_policys = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2026-01-20").cast("date")) | (~F.col("status").isin("LAPSED", "CANCELLED")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
    )
)

detail_rows = (
    recent_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status") == "APPROVED"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

ranked = (
    detail_rows
    .select(
        "*",
        F.round(F.percent_rank().over(w_end_date_year_by_end_date_ti), 3).alias("end_date_percent_rank"),
        F.first("end_date").over(w_end_date_year_by_end_date_fu).alias("first_end_date"),
        F.dense_rank().over(w_claim_id_holder_id_by_end_da).alias("end_date_dense_rank"),
        F.rank().over(w_claim_id_holder_id_by_end_da).alias("end_date_rank"),
        F.row_number().over(w_claim_id_holder_id_by_end_da_2).alias("rn"),
        F.lag("end_date", 2).over(w_claim_id_holder_id_by_end_da_2).alias("prev2_end_date"),
    )
)

rollup_base = (
    ranked
    .groupBy("end_date_dense_rank")
    .agg(
        F.max("end_date_percent_rank").alias("max_end_date_percent_rank"),
        F.count(F.when(F.col("claim_status") != "APPROVED", 1)).alias("n_claim_status_approved"),
        F.count("*").alias("cnt"),
        F.datediff(F.max("prev2_end_date"), F.min("prev2_end_date")).alias("prev2_end_date_span_days"),
    )
    .filter(F.col("cnt") >= 2)
)

annotated = (
    rollup_base
    .select(
        "end_date_dense_rank",
        "max_end_date_percent_rank",
        "cnt",
        "prev2_end_date_span_days",
        (F.col("end_date_dense_rank") + F.col("n_claim_status_approved")).alias("end_date_dense_rank_plus_n_claim_status_approved"),
        F.greatest(F.col("n_claim_status_approved"), F.col("cnt")).alias("greatest_n_claim_status_approved_cnt"),
    )
)

screened = (
    annotated
    .crossJoin(annotated.agg(F.avg("max_end_date_percent_rank").alias("overall_avg_max_end_date_percent_rank")))
    .filter(F.col("max_end_date_percent_rank") > F.col("overall_avg_max_end_date_percent_rank"))
    .drop("overall_avg_max_end_date_percent_rank")
)

enriched = (
    screened
    .select(
        "*",
        (F.col("end_date_dense_rank") - F.col("cnt")).alias("end_date_dense_rank_minus_cnt"),
    )
)

result = (
    enriched
    .select(
        "end_date_dense_rank",
        "max_end_date_percent_rank",
        "cnt",
        "prev2_end_date_span_days",
        "end_date_dense_rank_plus_n_claim_status_approved",
        "greatest_n_claim_status_approved_cnt",
    )
    .orderBy(
        F.col("end_date_dense_rank").desc(),
        F.col("greatest_n_claim_status_approved_cnt").asc(),
    )
)
