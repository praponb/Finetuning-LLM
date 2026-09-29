from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_risk_score = Window.orderBy(F.col("risk_score").asc_nulls_last(), "id")
w_segment_group_segment_by_ris = Window.partitionBy("segment_group", "segment").orderBy("risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_segment_group_segment = Window.partitionBy("segment_group", "segment")
w_joined_on_year_by_risk_score = Window.partitionBy(F.year("joined_on")).orderBy(F.col("risk_score_filled").desc_nulls_first())
w_all_by_risk_score_ties = Window.orderBy(F.col("risk_score").asc_nulls_last())
w_segment_group_segment_by_ris_2 = Window.partitionBy("segment_group", "segment").orderBy("risk_score", "id")
w_segment_segment_group_by_ris = Window.partitionBy("segment", "segment_group").orderBy(F.col("risk_score_filled").desc(), "id")
w_joined_on_year_by_risk_score_2 = Window.partitionBy(F.year("joined_on")).orderBy(F.col("risk_score_filled").desc_nulls_first(), "id")

prepared = (
    df_policyholders
    .filter((F.col("risk_score") <= 0.6) & (F.upper(F.trim(F.col("holder_name"))).like("N%")))
    .select(
        "id",
        "holder_name",
        "risk_score",
        "joined_on",
        "segment",
        F.when(F.col("segment").isin("VIP", "SME"), "group_a").when(F.col("segment") == "RETAIL", "group_b").otherwise("other").alias("segment_group"),
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.lead("risk_score_filled").over(w_all_by_risk_score).alias("next_risk_score_filled"),
        F.first("risk_score", ignorenulls=True).over(w_segment_group_segment_by_ris).alias("first_known_risk_score"),
        F.max("risk_score").over(w_segment_group_segment).alias("segment_group_segment_max_risk_score"),
        F.round(F.percent_rank().over(w_joined_on_year_by_risk_score), 3).alias("risk_score_filled_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_risk_score_ties), 3).alias("risk_score_percent_rank"),
        F.row_number().over(w_segment_group_segment_by_ris_2).alias("rn"),
        F.ntile(4).over(w_segment_group_segment_by_ris_2).alias("risk_score_quartile"),
        F.round(F.percent_rank().over(w_segment_segment_group_by_ris), 3).alias("risk_score_filled_percent_rank_2"),
        F.ntile(4).over(w_joined_on_year_by_risk_score_2).alias("risk_score_filled_quartile"),
    )
)

first_rows = (
    with_trends
    .filter((F.col("rn") <= 5) & (F.col("risk_score_quartile") >= 3))
    .drop("rn")
)

result = (
    first_rows
    .filter(F.col("risk_score_quartile") <= 8)
    .select(
        "id",
        "holder_name",
        "joined_on",
        "segment",
        "next_risk_score_filled",
        "first_known_risk_score",
        "segment_group_segment_max_risk_score",
        "risk_score_filled_percent_rank",
        "risk_score_percent_rank",
        "risk_score_quartile",
        "risk_score_filled_quartile",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
    )
)
