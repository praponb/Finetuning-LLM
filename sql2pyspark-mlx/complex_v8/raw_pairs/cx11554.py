from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_risk_score_moving_2 = Window.orderBy(F.col("risk_score").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_risk_score_running = Window.orderBy(F.col("risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_region_segment = Window.partitionBy("region", "segment")

scoped_holders = (
    df_policyholders
    .filter(F.col("risk_score").between(0.1, 0.6))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "segment",
        F.lower(F.regexp_replace(F.trim(F.col("segment")), " ", "_")).alias("segment_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("region")), " ", "_")).alias("region_slug"),
        F.lower(F.trim(F.col("segment"))).alias("segment_clean"),
    )
)

with_windows = (
    scoped_holders
    .select(
        "*",
        F.sum("risk_score").over(w_all_by_risk_score_moving_2).alias("risk_score_sum_3"),
        F.last("risk_score", ignorenulls=True).over(w_all_by_risk_score_running).alias("last_known_risk_score"),
        F.round(F.col("risk_score") - F.avg("risk_score").over(w_region_segment), 2).alias("risk_score_vs_region_segment_avg"),
    )
)

filtered = (
    with_windows.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.start_date") >= F.lit("2024-05-07").cast("date")), "left_semi")
)

result = (
    filtered
    .filter(F.col("region") == "North")
    .select(
        "id",
        "risk_score",
        "region_slug",
        "segment_clean",
        "last_known_risk_score",
        "risk_score_vs_region_segment_avg",
    )
    .orderBy(
        F.col("last_known_risk_score").desc(),
        F.col("risk_score_vs_region_segment_avg").asc_nulls_last(),
    )
)
