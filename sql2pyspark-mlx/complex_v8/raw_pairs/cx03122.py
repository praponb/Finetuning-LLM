from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_week_month_holder_r = Window.partitionBy(F.month("end_date_week"), "holder_region").orderBy("end_date_week", "id")
w_holder_segment_holder_region = Window.partitionBy("holder_segment", "holder_region").orderBy(F.col("end_date_week_cume_dist").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_segment_holder_region_2 = Window.partitionBy("holder_segment", "holder_region").orderBy(F.col("end_date_week_cume_dist").asc_nulls_last(), "id")
w_holder_region = Window.partitionBy("holder_region")
w_holder_segment_holder_region_3 = Window.partitionBy("holder_segment", "holder_region").orderBy(F.col("end_date_week_cume_dist").asc_nulls_last())
w_holder_region_end_date_week = Window.partitionBy("holder_region", "end_date_week_cume_dist_dense_rank").orderBy(F.col("holder_region_min_end_date_week_cume_dist").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_segment = Window.partitionBy("holder_segment")

filtered = (
    df_policies
    .filter(F.col("product") == "HEALTH")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.concat_ws(" / ", F.col("status"), F.col("product")).alias("status_product_label"),
        F.date_trunc("week", F.col("start_date")).alias("start_date_week"),
        F.date_trunc("week", F.col("end_date")).alias("end_date_week"),
    )
)

joined_holder = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.region").isin("Central", "East", "South"))
    .select(
        "s.id",
        "s.holder_id",
        "s.status_product_label",
        "s.end_date_week",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
    )
)

windowed = (
    joined_holder
    .select(
        "id",
        "status_product_label",
        "holder_segment",
        "holder_region",
        F.round(F.cume_dist().over(w_end_date_week_month_holder_r), 3).alias("end_date_week_cume_dist"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.nth_value("end_date_week_cume_dist", 2).over(w_holder_segment_holder_region).alias("second_end_date_week_cume_dist"),
        F.dense_rank().over(w_holder_segment_holder_region_2).alias("end_date_week_cume_dist_dense_rank"),
        F.min("end_date_week_cume_dist").over(w_holder_region).alias("holder_region_min_end_date_week_cume_dist"),
        F.round(F.percent_rank().over(w_holder_segment_holder_region_3), 3).alias("end_date_week_cume_dist_percent_rank"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        F.first("end_date_week_cume_dist").over(w_holder_region_end_date_week).alias("first_end_date_week_cume_dist"),
        F.count(F.lit(1)).over(w_holder_segment).alias("rows_in_holder_segment"),
    )
)

without_matches_2 = (
    with_windows
    .crossJoin(with_windows.agg(F.max("holder_region_min_end_date_week_cume_dist").alias("overall_max_holder_region_min_end_date_week_cume")))
    .filter(F.col("holder_region_min_end_date_week_cume_dist") < F.col("overall_max_holder_region_min_end_date_week_cume"))
    .drop("overall_max_holder_region_min_end_date_week_cume")
)

result = (
    without_matches_2
    .select(
        "id",
        "status_product_label",
        "holder_segment",
        "holder_region",
        "second_end_date_week_cume_dist",
        "holder_region_min_end_date_week_cume_dist",
        "end_date_week_cume_dist_percent_rank",
        "first_end_date_week_cume_dist",
        "rows_in_holder_segment",
    )
    .orderBy(
        F.col("id").asc(),
        F.col("holder_region_min_end_date_week_cume_dist").desc(),
    )
    .limit(10)
)
