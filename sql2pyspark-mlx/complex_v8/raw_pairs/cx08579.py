from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment = Window.partitionBy("segment")
w_all_by_risk_score_filled_run = Window.orderBy("risk_score_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("region").orderBy(F.col("segment_max_joined_on_month").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("region").orderBy(F.col("joined_on_month_share_of_segment").desc(), F.col("id").desc())

valid_holders = (
    df_policyholders
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
        F.month(F.col("joined_on")).alias("joined_on_month"),
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
    )
)

derived = (
    valid_holders
    .filter(F.col("joined_on_month").between(2, 9))
    .select(
        "id",
        "region",
        "segment",
        "joined_on_month",
        "risk_score_filled",
        F.concat_ws(" / ", F.col("holder_name"), F.col("segment")).alias("holder_name_segment_label"),
    )
)

ranked = (
    derived
    .select(
        "*",
        F.max("joined_on_month").over(w_segment).alias("segment_max_joined_on_month"),
        F.round(F.col("risk_score_filled") / F.nullif(F.sum("risk_score_filled").over(w_segment), F.lit(0)), 4).alias("risk_score_filled_share_of_segment"),
        F.round(F.col("joined_on_month") / F.nullif(F.sum("joined_on_month").over(w_segment), F.lit(0)), 4).alias("joined_on_month_share_of_segment"),
        F.first("joined_on_month", ignorenulls=True).over(w_all_by_risk_score_filled_run).alias("first_known_joined_on_month"),
    )
)

filtered = (
    ranked
    .crossJoin(ranked.agg(F.max("risk_score_filled_share_of_segment").alias("overall_max_risk_score_filled_share_of_segment")))
    .filter(F.col("risk_score_filled_share_of_segment") < F.col("overall_max_risk_score_filled_share_of_segment"))
    .drop("overall_max_risk_score_filled_share_of_segment")
)

unique_rows = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

unique_rows_2 = (
    unique_rows
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows_2
    .select(
        "id",
        "region",
        "segment",
        "joined_on_month",
        "risk_score_filled",
        "holder_name_segment_label",
        "segment_max_joined_on_month",
        "risk_score_filled_share_of_segment",
        "joined_on_month_share_of_segment",
        "first_known_joined_on_month",
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region_or_unknown"),
        F.length(F.trim(F.col("segment"))).alias("segment_len"),
    )
    .orderBy("region_or_unknown", F.col("joined_on_month_share_of_segment").desc())
)
