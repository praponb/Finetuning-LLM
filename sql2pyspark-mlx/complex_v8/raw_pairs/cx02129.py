from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region_segment_by_joined_on = Window.partitionBy("region", "segment").orderBy(F.col("joined_on").desc())
w_segment_region = Window.partitionBy("segment", "region")
w_joined_on_dense_rank = Window.partitionBy("joined_on_dense_rank")
w_region_by_joined_on_dense_ra = Window.partitionBy("region").orderBy(F.col("joined_on_dense_rank").desc(), "id")

cleaned = df_policyholders.filter(F.col("segment") != "SME")

windowed = (
    cleaned
    .select(
        "id",
        "region",
        "risk_score",
        "joined_on",
        F.dense_rank().over(w_region_segment_by_joined_on).alias("joined_on_dense_rank"),
        F.round(F.col("risk_score") / F.nullif(F.sum("risk_score").over(w_segment_region), F.lit(0)), 4).alias("risk_score_share_of_segment_region"),
        F.sum("risk_score").over(w_segment_region).alias("segment_region_sum_risk_score"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.count(F.lit(1)).over(w_joined_on_dense_rank).alias("rows_in_joined_on_dense_rank"),
        F.lag("joined_on_dense_rank").over(w_region_by_joined_on_dense_ra).alias("prev_joined_on_dense_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "region",
        "joined_on",
        "joined_on_dense_rank",
        "risk_score_share_of_segment_region",
        "segment_region_sum_risk_score",
        "rows_in_joined_on_dense_rank",
        "prev_joined_on_dense_rank",
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region_or_unknown"),
        F.when(F.col("region").isin("West", "South"), "group_a").when(F.col("region") == "North", "group_b").otherwise("other").alias("region_group"),
    )
    .orderBy(F.col("joined_on_dense_rank").asc())
)
