from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_cnt_full = Window.orderBy("cnt", "joined_on_dayofweek", "joined_on_ym", "segment").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_cnt = Window.orderBy("cnt", "joined_on_dayofweek", "joined_on_ym", "segment")
w_n_region_south_least_cnt_n_r = Window.partitionBy("n_region_south", "least_cnt_n_region_south")
w_joined_on_dayofweek = Window.partitionBy("joined_on_dayofweek")
w_n_region_south_cnt_by_second = Window.partitionBy("n_region_south", "cnt").orderBy("second_cnt", "joined_on_dayofweek", "joined_on_ym", "segment")

recent_holders = (
    df_policyholders
    .filter(F.col("holder_name").isNotNull())
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.dayofweek(F.col("joined_on")).alias("joined_on_dayofweek"),
        F.round(F.col("risk_score") * 0.25, 2).alias("risk_score_adj"),
        F.round(F.col("risk_score") * 1.1, 2).alias("risk_score_adj_2"),
    )
)

rollup_base = (
    recent_holders
    .groupBy(
        "joined_on_dayofweek",
        F.date_format("joined_on", "yyyy-MM").alias("joined_on_ym"),
        "segment",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("risk_score").alias("max_risk_score"),
        F.min("joined_on").alias("first_joined_on"),
        F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("n_region_south"),
        F.countDistinct("region").alias("distinct_regions"),
    )
)

derived = (
    rollup_base
    .filter(F.col("distinct_regions") <= 8)
    .withColumn("least_cnt_n_region_south", F.least(F.col("cnt"), F.col("n_region_south")))
)

with_trends = (
    derived
    .select(
        "*",
        F.nth_value("cnt", 2).over(w_all_by_cnt_full).alias("second_cnt"),
        F.ntile(5).over(w_all_by_cnt).alias("cnt_quintile"),
        F.count(F.lit(1)).over(w_n_region_south_least_cnt_n_r).alias("rows_in_n_region_south_least_cnt_n_region_south"),
    )
)

filtered = (
    with_trends
    .crossJoin(with_trends.agg(F.avg("n_region_south").alias("overall_avg_n_region_south")))
    .filter(F.col("n_region_south") > F.col("overall_avg_n_region_south"))
    .drop("overall_avg_n_region_south")
)

with_windows = (
    filtered
    .select(
        "joined_on_dayofweek",
        "joined_on_ym",
        "segment",
        "cnt",
        "max_risk_score",
        "distinct_regions",
        "least_cnt_n_region_south",
        "cnt_quintile",
        F.min("cnt").over(w_joined_on_dayofweek).alias("joined_on_dayofweek_min_cnt"),
        F.size(F.collect_set("segment").over(w_joined_on_dayofweek)).alias("distinct_segment_in_joined_on_dayofweek"),
        F.dense_rank().over(w_n_region_south_cnt_by_second).alias("second_cnt_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "joined_on_dayofweek",
        "joined_on_ym",
        "segment",
        "cnt",
        "max_risk_score",
        "least_cnt_n_region_south",
        "cnt_quintile",
        "joined_on_dayofweek_min_cnt",
        "distinct_segment_in_joined_on_dayofweek",
        F.round(F.col("max_risk_score") * 1.5, 2).alias("max_risk_score_adj"),
    )
    .orderBy(
        "max_risk_score",
        "joined_on_dayofweek",
        "joined_on_ym",
        "segment",
    )
    .limit(5)
)
