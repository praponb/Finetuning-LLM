from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh = Window.orderBy(F.col("kwh").asc_nulls_last(), "id")
w_all_by_kwh_ties = Window.orderBy(F.col("kwh").asc_nulls_last())
w_site_id = Window.partitionBy("site_id")
w_all_by_read_at_moving_2 = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_quality_site_id_by_read_at = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at").desc(), "id")
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_quality_site_id_by_read_at_p = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(-6, -1)
w_kwh_rank_by_read_at_percent = Window.partitionBy("kwh_rank").orderBy(F.col("read_at_percent_rank").desc(), "id").rowsBetween(-3, Window.currentRow)
w_kwh_rank = Window.partitionBy("kwh_rank")
w_quality_by_kwh_vs_site_id_av = Window.partitionBy("quality").orderBy(F.col("kwh_vs_site_id_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_readings = (
    df_meter_readings
    .filter((F.col("kwh") < 89.8) & (F.col("voltage") <= 234.7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

ranked = (
    cleaned_readings
    .select(
        "id",
        "quality",
        F.lead("read_at", 2).over(w_all_by_kwh).alias("next2_read_at"),
        F.round(F.percent_rank().over(w_all_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.rank().over(w_all_by_kwh_ties).alias("kwh_rank"),
        F.lead("read_at", 3).over(w_all_by_kwh).alias("next3_read_at"),
        F.round(F.avg("kwh").over(w_all_by_read_at_moving_2), 2).alias("kwh_avg_3"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.min("kwh").over(w_site_id).alias("site_id_min_kwh"),
        F.lag("read_at", 3).over(w_quality_site_id_by_read_at).alias("prev3_read_at"),
        F.round(F.percent_rank().over(w_all_by_read_at), 3).alias("read_at_percent_rank"),
        F.round(F.avg("kwh").over(w_quality_site_id_by_read_at_p), 2).alias("kwh_avg_6_prior"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "quality",
        "next2_read_at",
        "kwh_share_of_site_id",
        "next3_read_at",
        "kwh_avg_3",
        "kwh_vs_site_id_avg",
        "site_id_min_kwh",
        "prev3_read_at",
        "kwh_avg_6_prior",
        F.max("kwh_vs_site_id_avg").over(w_kwh_rank_by_read_at_percent).alias("kwh_vs_site_id_avg_max_4"),
        F.size(F.collect_set("quality").over(w_kwh_rank)).alias("distinct_quality_in_kwh_rank"),
        F.count(F.lit(1)).over(w_kwh_rank).alias("rows_in_kwh_rank"),
        F.first("kwh_avg_3").over(w_quality_by_kwh_vs_site_id_av).alias("first_kwh_avg_3"),
        F.min("kwh_percent_rank").over(w_kwh_rank).alias("kwh_rank_min_kwh_percent_rank"),
        F.nth_value("kwh_rank", 2).over(w_quality_by_kwh_vs_site_id_av).alias("second_kwh_rank"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "kwh_share_of_site_id",
        "kwh_avg_3",
        "kwh_vs_site_id_avg",
        "site_id_min_kwh",
        "prev3_read_at",
        "kwh_avg_6_prior",
        "kwh_vs_site_id_avg_max_4",
        "distinct_quality_in_kwh_rank",
        "rows_in_kwh_rank",
        "first_kwh_avg_3",
        "kwh_rank_min_kwh_percent_rank",
        "second_kwh_rank",
        F.when(F.col("kwh_vs_site_id_avg") >= 60.0, "HIGH").when(F.col("kwh_vs_site_id_avg") >= -48.0, "MEDIUM").otherwise("LOW").alias("kwh_vs_site_id_avg_tier"),
        F.round(F.col("rows_in_kwh_rank") / F.nullif(F.col("site_id_min_kwh"), F.lit(0)), 3).alias("rows_in_kwh_rank_per_site_id_min_kwh"),
    )
    .orderBy("rows_in_kwh_rank_per_site_id_min_kwh", "distinct_quality_in_kwh_rank")
)
