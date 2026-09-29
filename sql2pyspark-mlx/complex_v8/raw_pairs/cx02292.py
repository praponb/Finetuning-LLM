from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_joined_on_year = Window.partitionBy(F.year("joined_on"))
w_all_by_risk_score = Window.orderBy("risk_score", "id")
w_segment_by_joined_on_ties = Window.partitionBy("segment").orderBy(F.col("joined_on").desc())
w_all_by_distinct_region_in_jo = Window.orderBy(F.col("distinct_region_in_joined_on_year").desc(), "id")
w_seq_no_distinct_region_in_jo = Window.partitionBy("seq_no", "distinct_region_in_joined_on_year")
w_all_by_distinct_region_in_jo_2 = Window.orderBy(F.col("distinct_region_in_joined_on_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_region_in_joined_on = Window.partitionBy("distinct_region_in_joined_on_year").orderBy(F.col("distinct_region_in_joined_on_year").desc(), "id")

base = (
    df_policyholders
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
    )
)

with_trends = (
    base
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.size(F.collect_set("region").over(w_joined_on_year)).alias("distinct_region_in_joined_on_year"),
        (F.col("risk_score") - F.lag("risk_score").over(w_all_by_risk_score)).alias("risk_score_change"),
        F.round(F.col("risk_score") - F.avg("risk_score").over(w_joined_on_year), 2).alias("risk_score_vs_joined_on_year_avg"),
        F.row_number().over(w_all_by_risk_score).alias("seq_no"),
        F.round(F.cume_dist().over(w_segment_by_joined_on_ties), 3).alias("joined_on_cume_dist"),
    )
)

sequenced = (
    with_trends
    .select(
        "*",
        (F.col("risk_score_change") - F.lag("risk_score_change").over(w_all_by_distinct_region_in_jo)).alias("risk_score_change_change"),
        F.min("risk_score").over(w_seq_no_distinct_region_in_jo).alias("seq_no_distinct_region_in_joined_on_year_min_ris"),
        F.nth_value("joined_on_cume_dist", 2).over(w_all_by_distinct_region_in_jo_2).alias("second_joined_on_cume_dist"),
        F.nth_value("joined_on", 2).over(w_all_by_distinct_region_in_jo_2).alias("second_joined_on"),
        F.ntile(3).over(w_distinct_region_in_joined_on).alias("distinct_region_in_joined_on_year_tercile"),
    )
)

top_per_group = sequenced.filter(F.col("seq_no") == 1)

result = (
    top_per_group
    .filter(F.col("distinct_region_in_joined_on_year") < 8)
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        "distinct_region_in_joined_on_year",
        "risk_score_change",
        "risk_score_vs_joined_on_year_avg",
        "seq_no",
        "joined_on_cume_dist",
        "risk_score_change_change",
        "second_joined_on",
    )
    .orderBy(F.col("risk_score").desc_nulls_first(), F.col("joined_on").desc())
)
