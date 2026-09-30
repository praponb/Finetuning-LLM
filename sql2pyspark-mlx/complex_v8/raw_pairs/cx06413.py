from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_year = Window.partitionBy(F.year("start_date"))
w_holder_segment_by_premium_bu = Window.partitionBy("holder_segment").orderBy(F.col("premium_bucket").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_start_date_month_holder_regi = Window.partitionBy(F.month("start_date"), "holder_region").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_holder_risk_score = Window.orderBy(F.col("holder_risk_score").desc(), "id")
w_holder_segment_by_premium_bu_2 = Window.partitionBy("holder_segment").orderBy(F.col("premium_bucket").asc_nulls_last())
w_start_date_month_holder_regi_2 = Window.partitionBy(F.month("start_date"), "holder_region").orderBy("end_date", "id").rowsBetween(-1, -1)

filtered = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-12-02").cast("date")) & (F.col("status") == "EXPIRED"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

enriched_holder = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.premium_bucket",
        "s.status_ref",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

with_windows = (
    enriched_holder
    .select(
        "*",
        F.round(F.col("holder_risk_score") - F.avg("holder_risk_score").over(w_start_date_year), 2).alias("holder_risk_score_vs_start_date_year_avg"),
        F.round(F.avg("premium_bucket").over(w_holder_segment_by_premium_bu), 2).alias("premium_bucket_avg_1_prior"),
        F.round(F.col("premium_bucket") - F.avg("premium_bucket").over(w_start_date_year), 2).alias("premium_bucket_vs_start_date_year_avg"),
        F.first("holder_risk_score").over(w_start_date_month_holder_regi).alias("first_holder_risk_score"),
        F.round(F.avg("premium_bucket").over(w_start_date_year), 2).alias("start_date_year_avg_premium_bucket"),
        F.sum("premium_bucket").over(w_start_date_year).alias("start_date_year_sum_premium_bucket"),
        F.row_number().over(w_all_by_holder_risk_score).alias("seq_no"),
        F.round(F.avg("holder_risk_score").over(w_start_date_year), 2).alias("start_date_year_avg_holder_risk_score"),
        F.sum("holder_risk_score").over(w_holder_segment_by_premium_bu).alias("holder_risk_score_sum_1_prior"),
        F.dense_rank().over(w_holder_segment_by_premium_bu_2).alias("premium_bucket_dense_rank"),
        F.round(F.avg("premium_bucket").over(w_start_date_month_holder_regi_2), 2).alias("premium_bucket_avg_1_prior_2"),
    )
)

summary = (
    with_windows
    .filter(F.col("premium_bucket_avg_1_prior_2") < 3560.0)
    .groupBy("premium_bucket_dense_rank", "seq_no")
    .agg(
        F.round(F.avg("premium_bucket_vs_start_date_year_avg"), 2).alias("avg_premium_bucket_vs_start_date_year_avg"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 3)
)

result = (
    summary
    .select(
        "premium_bucket_dense_rank",
        "seq_no",
        "n_rows",
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket"),
    )
    .orderBy(F.col("n_rows").desc())
)
