from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_segments = Window.partitionBy("distinct_segments")
w_segment_by_n_region_north_mo = Window.partitionBy("segment").orderBy("n_region_north", "region", "segment").rowsBetween(-1, Window.currentRow)
w_row_count_by_n_region_north = Window.partitionBy("row_count").orderBy("n_region_north", "region", "segment")
w_n_region_north_row_count = Window.partitionBy("n_region_north", "row_count")
w_segment_distinct_segment_in = Window.partitionBy("segment", "distinct_segment_in_distinct_segments").orderBy("n_region_north")

base_holders = (
    df_policyholders
    .filter(F.col("holder_name").isNotNull())
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
        F.when(F.col("segment").isin("VIP", "RETAIL"), "group_a").when(F.col("segment") == "SME", "group_b").otherwise("other").alias("segment_group"),
        F.concat_ws(": ", F.col("region"), F.col("holder_name")).alias("region_holder_name_label"),
        F.when(F.col("segment").isin("SME", "RETAIL"), "group_a").when(F.col("segment") == "VIP", "group_b").otherwise("other").alias("segment_group_2"),
    )
)

rollup_base = (
    base_holders
    .groupBy("region", "segment")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("segment_group_2").alias("distinct_segment_group_2s"),
        F.round(100 * F.count(F.when(F.col("segment").isin("RETAIL", "SME"), 1)) / F.count("*"), 1).alias("pct_segment_in_retail_sme"),
        F.sum(F.when(F.col("region") != "North", 1).otherwise(0)).alias("n_region_north"),
        F.countDistinct("segment").alias("distinct_segments"),
    )
    .filter(F.col("row_count") >= 2)
)

qualified = (
    rollup_base
    .crossJoin(rollup_base.agg(F.max("n_region_north").alias("overall_max_n_region_north")))
    .filter(F.col("n_region_north") < F.col("overall_max_n_region_north"))
    .drop("overall_max_n_region_north")
)

with_windows = (
    qualified
    .select(
        "region",
        "segment",
        "row_count",
        "pct_segment_in_retail_sme",
        "n_region_north",
        "distinct_segments",
        F.size(F.collect_set("segment").over(w_distinct_segments)).alias("distinct_segment_in_distinct_segments"),
        F.max("pct_segment_in_retail_sme").over(w_segment_by_n_region_north_mo).alias("pct_segment_in_retail_sme_max_2"),
        F.row_number().over(w_row_count_by_n_region_north).alias("seq_no"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.size(F.collect_set("region").over(w_n_region_north_row_count)).alias("distinct_region_in_n_region_north_row_count"),
        F.dense_rank().over(w_segment_distinct_segment_in).alias("n_region_north_dense_rank"),
        F.round(F.col("distinct_segments") / F.nullif(F.sum("distinct_segments").over(w_n_region_north_row_count), F.lit(0)), 4).alias("distinct_segments_share_of_n_region_north_row_co"),
        F.round(F.col("n_region_north") / F.nullif(F.sum("n_region_north").over(w_n_region_north_row_count), F.lit(0)), 4).alias("n_region_north_share_of_n_region_north_row_count"),
        F.round(F.col("row_count") - F.avg("row_count").over(w_n_region_north_row_count), 2).alias("row_count_vs_n_region_north_row_count_avg"),
    )
)

result = (
    windowed
    .filter(F.col("pct_segment_in_retail_sme_max_2") <= 195.0)
    .select(
        "region",
        "segment",
        "row_count",
        "pct_segment_in_retail_sme",
        "n_region_north",
        "distinct_segment_in_distinct_segments",
        "pct_segment_in_retail_sme_max_2",
        "seq_no",
        "distinct_region_in_n_region_north_row_count",
        "n_region_north_dense_rank",
        "n_region_north_share_of_n_region_north_row_count",
        "row_count_vs_n_region_north_row_count_avg",
        F.length(F.trim(F.col("segment"))).alias("segment_len"),
        F.coalesce(F.col("segment"), F.lit("UNKNOWN")).alias("segment_or_unknown"),
    )
)
