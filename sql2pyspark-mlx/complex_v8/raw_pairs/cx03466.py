from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment_joined_on_month = Window.partitionBy("segment", F.date_trunc("month", "joined_on"))
w_joined_on_month_end_month_by = Window.partitionBy(F.date_trunc("month", "joined_on_month_end")).orderBy("joined_on", "id")
w_joined_on_year_n_segment_sme = Window.partitionBy("joined_on_year", "n_segment_sme").orderBy(F.col("risk_score_segment_sme").desc(), "joined_on_year", "prev_joined_on_month_end_ym")
w_joined_on_year_by_avg_risk_s = Window.partitionBy("joined_on_year").orderBy("avg_risk_score", "joined_on_year", "prev_joined_on_month_end_ym")

prepared = (
    df_policyholders
    .filter((F.col("segment").isin("RETAIL", "VIP")) | (F.upper(F.trim(F.col("holder_name"))).like("E%")))
    .select(
        "id",
        "holder_name",
        "risk_score",
        "joined_on",
        "segment",
        F.last_day(F.col("joined_on")).alias("joined_on_month_end"),
    )
)

with_trends = (
    prepared
    .select(
        "id",
        "risk_score",
        "joined_on",
        "segment",
        F.sum("risk_score").over(w_segment_joined_on_month).alias("segment_joined_on_month_sum_risk_score"),
        F.ntile(4).over(w_joined_on_month_end_month_by).alias("joined_on_quartile"),
        F.lag("joined_on_month_end").over(w_joined_on_month_end_month_by).alias("prev_joined_on_month_end"),
    )
)

totals = (
    with_trends
    .filter(F.col("prev_joined_on_month_end").isNotNull())
    .groupBy(
        F.year("joined_on").alias("joined_on_year"),
        F.date_format("prev_joined_on_month_end", "yyyy-MM").alias("prev_joined_on_month_end_ym"),
    )
    .agg(
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
        F.max("prev_joined_on_month_end").alias("last_prev_joined_on_month_end"),
        F.round(100 * F.count(F.when(F.col("segment").isNotNull(), 1)) / F.count("*"), 1).alias("pct_segment_is_not_null"),
        F.count(F.when(F.col("segment") == "SME", 1)).alias("n_segment_sme"),
        F.sum(F.when(F.col("segment") == "SME", F.col("risk_score"))).alias("risk_score_segment_sme"),
    )
)

sequenced = (
    totals
    .select(
        "*",
        (F.col("risk_score_segment_sme") - F.lag("risk_score_segment_sme").over(w_joined_on_year_n_segment_sme)).alias("risk_score_segment_sme_change"),
        F.lag("avg_risk_score").over(w_joined_on_year_n_segment_sme).alias("prev_avg_risk_score"),
        F.row_number().over(w_joined_on_year_by_avg_risk_s).alias("seq_no"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["n_segment_sme", "seq_no"], ["n_segment_sme"], []],
        "n_segment_sme", "seq_no",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("n_segment_sme").alias("min_n_segment_sme"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("n_segment_sme").asc_nulls_last(),
        F.col("seq_no").asc_nulls_last(),
    )
)
