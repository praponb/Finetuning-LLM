from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_kwh_tier = Window.partitionBy("kwh_tier")
w_quality = Window.partitionBy("quality")
w_read_at_year_kwh_tier_by_kwh = Window.partitionBy(F.year("read_at"), "kwh_tier").orderBy(F.col("kwh").desc(), "id")
w_all_by_read_at_full = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id = Window.partitionBy("site_id")
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_read_at_year_kwh_tier_by_kwh_2 = Window.partitionBy(F.year("read_at"), "kwh_tier").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_distinct_quality_in_site_id = Window.partitionBy("distinct_quality_in_site_id", F.to_date("last_read_at"))
w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy("read_at")
w_all_by_last_read_at_ties = Window.orderBy("last_read_at")
w_all_by_last_read_at = Window.orderBy("last_read_at", "id")

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 106.5, "HIGH").when(F.col("kwh") >= 45.0, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
        F.when(F.col("voltage") >= 232.4, "HIGH").when(F.col("voltage") >= 222.1, "MEDIUM").otherwise("LOW").alias("voltage_tier"),
    )
)

with_trends = (
    scoped
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "voltage_tier",
        F.sum("kwh").over(w_kwh_tier).alias("kwh_tier_sum_kwh"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        (F.col("kwh") - F.lag("kwh").over(w_read_at_year_kwh_tier_by_kwh)).alias("kwh_change"),
        F.nth_value("kwh", 2).over(w_all_by_read_at_full).alias("second_kwh"),
        F.max("kwh").over(w_site_id).alias("site_id_max_kwh"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.lag("read_at", 3).over(w_all_by_kwh).alias("prev3_read_at"),
        F.last("read_at").over(w_read_at_year_kwh_tier_by_kwh_2).alias("last_read_at"),
        F.max("kwh").over(w_quality).alias("quality_max_kwh"),
        F.lag("voltage", 1, 0).over(w_all_by_read_at).alias("prev_voltage"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "voltage_tier",
        "kwh_tier_sum_kwh",
        "rows_in_quality",
        "kwh_change",
        "second_kwh",
        "site_id_max_kwh",
        "distinct_quality_in_site_id",
        "kwh_share_of_site_id",
        "prev3_read_at",
        "last_read_at",
        "quality_max_kwh",
        F.round(F.avg("quality_max_kwh").over(w_distinct_quality_in_site_id), 2).alias("distinct_quality_in_site_id_last_read_at_to_date"),
        F.min("kwh_tier_sum_kwh").over(w_distinct_quality_in_site_id).alias("distinct_quality_in_site_id_last_read_at_to_date_2"),
        F.round(F.percent_rank().over(w_site_id_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_last_read_at_ties), 3).alias("last_read_at_cume_dist"),
        F.lead("rows_in_quality").over(w_all_by_last_read_at).alias("next_rows_in_quality"),
        (F.col("kwh_tier_sum_kwh") - F.lag("kwh_tier_sum_kwh").over(w_all_by_last_read_at)).alias("kwh_tier_sum_kwh_change"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "voltage_tier",
        "kwh_tier_sum_kwh",
        "second_kwh",
        "site_id_max_kwh",
        "distinct_quality_in_site_id",
        "kwh_share_of_site_id",
        "last_read_at",
        "quality_max_kwh",
        "distinct_quality_in_site_id_last_read_at_to_date",
        "read_at_percent_rank",
        "last_read_at_cume_dist",
        "next_rows_in_quality",
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
        F.when(F.col("voltage_tier").isin("MEDIUM", "LOW"), "group_a").when(F.col("voltage_tier") == "HIGH", "group_b").otherwise("other").alias("voltage_tier_group"),
    )
    .orderBy(F.col("site_id_max_kwh").asc_nulls_last())
)
