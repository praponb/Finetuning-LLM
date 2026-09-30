from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt = Window.partitionBy("cnt")
w_all_by_days_since_end_date_h = Window.orderBy(F.col("days_since_end_date_holder_region_is_not_null").asc_nulls_last())
w_status_or_unknown_distinct_h = Window.partitionBy("status_or_unknown", "distinct_holder_segments").orderBy("days_since_end_date_holder_region_is_not_null", "product", "status_or_unknown", "holder_region")
w_status_or_unknown_distinct_h_2 = Window.partitionBy("status_or_unknown", "distinct_holder_segments").orderBy("days_since_end_date_holder_region_is_not_null", "product", "status_or_unknown", "holder_region").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_days_since_end_date_h_2 = Window.orderBy("days_since_end_date_holder_region_is_not_null", "product", "status_or_unknown", "holder_region")
w_distinct_status_or_unknowns = Window.partitionBy("distinct_status_or_unknowns").orderBy("days_since_end_date_holder_region_is_not_null_de", "product", "status_or_unknown", "holder_region").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_status_or_unknowns_2 = Window.partitionBy("distinct_status_or_unknowns").orderBy("days_since_end_date_holder_region_is_not_null_de", "product", "status_or_unknown", "holder_region")

prepared_policys = (
    df_policies
    .filter(F.col("start_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.date_trunc("quarter", F.col("end_date")).alias("end_date_quarter"),
    )
)

combined_details_holder = (
    prepared_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.holder_name").isNotNull())
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        "s.status_or_unknown",
        "s.days_since_end_date",
        "s.end_date_quarter",
        F.col("p.segment").alias("holder_segment"),
        "p.holder_name",
        F.col("p.region").alias("holder_region"),
    )
)

stats = (
    combined_details_holder
    .groupBy("product", "status_or_unknown", "holder_region")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("holder_segment").alias("distinct_holder_segments"),
        F.countDistinct("status_or_unknown").alias("distinct_status_or_unknowns"),
        F.sum("days_since_end_date").alias("total_days_since_end_date"),
        F.sum(F.when(F.col("holder_region").isNotNull(), F.col("days_since_end_date"))).alias("days_since_end_date_holder_region_is_not_null"),
        F.round(F.stddev("days_since_end_date"), 2).alias("stddev_days_since_end_date"),
    )
)

without_matches = (
    stats
    .crossJoin(stats.agg(F.min("cnt").alias("overall_min_cnt")))
    .filter(F.col("cnt") > F.col("overall_min_cnt"))
    .drop("overall_min_cnt")
)

derived = (
    without_matches
    .select(
        "*",
        F.coalesce(F.col("holder_region"), F.lit("UNKNOWN")).alias("holder_region_or_unknown"),
        F.abs(F.col("cnt") - 6).alias("cnt_distance"),
    )
)

with_windows = (
    derived
    .select(
        "product",
        "status_or_unknown",
        "holder_region",
        "cnt",
        "distinct_holder_segments",
        "distinct_status_or_unknowns",
        "total_days_since_end_date",
        "days_since_end_date_holder_region_is_not_null",
        "stddev_days_since_end_date",
        "holder_region_or_unknown",
        F.size(F.collect_set("product").over(w_cnt)).alias("distinct_product_in_cnt"),
        F.dense_rank().over(w_all_by_days_since_end_date_h).alias("days_since_end_date_holder_region_is_not_null_de"),
        F.row_number().over(w_status_or_unknown_distinct_h).alias("seq_no"),
        F.first("total_days_since_end_date").over(w_status_or_unknown_distinct_h_2).alias("first_total_days_since_end_date"),
    )
)

ranked = (
    with_windows
    .select(
        "*",
        F.lead("distinct_product_in_cnt", 2, 0).over(w_all_by_days_since_end_date_h_2).alias("next2_distinct_product_in_cnt"),
        F.ntile(4).over(w_all_by_days_since_end_date_h_2).alias("days_since_end_date_holder_region_is_not_null_qu"),
        F.nth_value("seq_no", 2).over(w_distinct_status_or_unknowns).alias("second_seq_no"),
        F.nth_value("total_days_since_end_date", 2).over(w_distinct_status_or_unknowns).alias("second_total_days_since_end_date"),
        F.rank().over(w_distinct_status_or_unknowns_2).alias("days_since_end_date_holder_region_is_not_null_de_2"),
    )
)

result = (
    ranked
    .select(
        "product",
        "distinct_holder_segments",
        "total_days_since_end_date",
        "days_since_end_date_holder_region_is_not_null",
        "stddev_days_since_end_date",
        "holder_region_or_unknown",
        "days_since_end_date_holder_region_is_not_null_de",
        "seq_no",
        "first_total_days_since_end_date",
        "days_since_end_date_holder_region_is_not_null_qu",
        "second_seq_no",
        "days_since_end_date_holder_region_is_not_null_de_2",
    )
    .orderBy("days_since_end_date_holder_region_is_not_null_de_2")
)
