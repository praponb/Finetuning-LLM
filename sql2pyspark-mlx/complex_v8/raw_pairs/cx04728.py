from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment_joined_on_year_by_jo = Window.partitionBy("segment", F.year("joined_on")).orderBy("joined_on_plus_90d", "id").rowsBetween(-2, Window.currentRow)
w_segment_joined_on_year_by_jo_2 = Window.partitionBy("segment", F.year("joined_on")).orderBy("joined_on_plus_90d")
w_segment_joined_on_quarter = Window.partitionBy("segment", "joined_on_quarter")
w_joined_on_quarter_joined_on = Window.partitionBy("joined_on_quarter", F.year("joined_on")).orderBy("joined_on_quarter", "id")

scoped = (
    df_policyholders
    .select(
        "id",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.quarter(F.col("joined_on")).alias("joined_on_quarter"),
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
        F.date_add(F.col("joined_on"), 90).alias("joined_on_plus_90d"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.sum("joined_on_quarter").over(w_segment_joined_on_year_by_jo).alias("joined_on_quarter_sum_3"),
        F.round(F.percent_rank().over(w_segment_joined_on_year_by_jo_2), 3).alias("joined_on_plus_90d_percent_rank"),
        F.sum("joined_on_quarter").over(w_segment_joined_on_quarter).alias("segment_joined_on_quarter_sum_joined_on_quarter"),
        F.lag("joined_on").over(w_joined_on_quarter_joined_on).alias("prev_joined_on"),
    )
)

filtered = (
    with_windows.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.status") == "LAPSED"), "left_anti")
    .filter(F.col("prev_joined_on") >= F.lit("2024-03-22").cast("date"))
)

result = (
    filtered
    .groupBy("region")
    .pivot("segment", ["VIP", "RETAIL", "SME"])
    .agg(F.avg("risk_score"))
    .orderBy("region")
)
