from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment = Window.partitionBy("segment")
w_joined_on_month_region_by_jo = Window.partitionBy(F.date_trunc("month", "joined_on"), "region").orderBy("joined_on", "id")

valid = df_policyholders.filter(F.col("risk_score") > 0.3)

with_windows = (
    valid
    .select(
        "*",
        F.sum("risk_score").over(w_segment).alias("segment_sum_risk_score"),
        F.row_number().over(w_joined_on_month_region_by_jo).alias("seq_no"),
        F.count(F.lit(1)).over(w_segment).alias("rows_in_segment"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        "segment_sum_risk_score",
        "seq_no",
        "rows_in_segment",
        F.concat_ws("|", F.col("holder_name"), F.col("segment")).alias("holder_name_segment_label"),
        F.add_months(F.col("joined_on"), 12).alias("joined_on_plus_12m"),
    )
)

result = (
    enriched
    .select(
        "id",
        "holder_name",
        "region",
        "segment",
        "segment_sum_risk_score",
        "rows_in_segment",
        "holder_name_segment_label",
        "joined_on_plus_12m",
        F.date_add(F.col("joined_on_plus_12m"), 7).alias("joined_on_plus_12m_plus_7d"),
    )
    .orderBy("region")
)
