from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_week_month_by_kwh = Window.partitionBy(F.month("read_at_week")).orderBy(F.col("kwh").desc(), "id")
w_all_by_rn_prior_6 = Window.orderBy("rn", "id").rowsBetween(-6, -1)
w_rn_quality_len_by_prev_volta = Window.partitionBy("rn", "quality_len").orderBy(F.col("prev_voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn = Window.partitionBy("rn")
w_rn_quality_len_by_prev_volta_2 = Window.partitionBy("rn", "quality_len").orderBy(F.col("prev_voltage").desc(), "id").rowsBetween(-6, -1)

recent = (
    df_meter_readings
    .filter((F.col("quality") == "OK") | (F.col("voltage") >= 219.5))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

ranked = (
    recent
    .select(
        "*",
        F.lead("quality_len").over(w_read_at_week_month_by_kwh).alias("next_quality_len"),
        F.row_number().over(w_read_at_week_month_by_kwh).alias("rn"),
        F.lag("voltage", 1, 0).over(w_read_at_week_month_by_kwh).alias("prev_voltage"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "read_at_week",
        "quality_len",
        "next_quality_len",
        F.round(F.avg("prev_voltage").over(w_all_by_rn_prior_6), 2).alias("prev_voltage_avg_6_prior"),
        F.first("next_quality_len", ignorenulls=True).over(w_rn_quality_len_by_prev_volta).alias("first_known_next_quality_len"),
        F.size(F.collect_set("site_id").over(w_rn)).alias("distinct_site_id_in_rn"),
        F.round(F.avg("kwh").over(w_rn_quality_len_by_prev_volta_2), 2).alias("kwh_avg_6_prior"),
        F.sum("next_quality_len").over(w_rn).alias("rn_sum_next_quality_len"),
    )
)

stats = (
    ranked_2
    .groupBy("quality", "distinct_site_id_in_rn")
    .agg(
        F.sum(F.when(F.col("quality") == "OK", 1).otherwise(0)).alias("n_quality_ok"),
        F.count("*").alias("cnt"),
        F.max("first_known_next_quality_len").alias("max_first_known_next_quality_len"),
        F.percentile_approx("distinct_site_id_in_rn", 0.5).alias("median_distinct_site_id_in_rn"),
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
)

labeled = (
    stats
    .select(
        "quality",
        "distinct_site_id_in_rn",
        "n_quality_ok",
        "cnt",
        "distinct_qualitys",
        "quality_list",
        F.round(F.col("distinct_qualitys") / F.nullif(F.col("cnt"), F.lit(0)), 3).alias("distinct_qualitys_per_cnt"),
        F.array_join(F.array_sort(F.col("quality_list")), ",").alias("quality_list_text"),
        F.least(F.col("distinct_site_id_in_rn"), F.col("cnt")).alias("least_distinct_site_id_in_rn_cnt"),
    )
)

filtered = (
    labeled
    .crossJoin(labeled.agg(F.min("distinct_qualitys_per_cnt").alias("overall_min_distinct_qualitys_per_cnt")))
    .filter(F.col("distinct_qualitys_per_cnt") > F.col("overall_min_distinct_qualitys_per_cnt"))
    .drop("overall_min_distinct_qualitys_per_cnt")
)

flattened = (
    filtered
    .select(
        "quality",
        "distinct_site_id_in_rn",
        "n_quality_ok",
        "cnt",
        "distinct_qualitys",
        "distinct_qualitys_per_cnt",
        "quality_list_text",
        "least_distinct_site_id_in_rn_cnt",
        F.posexplode("quality_list").alias("quality_list_item_pos", "quality_list_item"),
    )
)

result = (
    flattened
    .select(
        "quality",
        "distinct_site_id_in_rn",
        "cnt",
        "distinct_qualitys",
        "quality_list_text",
        "least_distinct_site_id_in_rn_cnt",
        "quality_list_item_pos",
        "quality_list_item",
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.least(F.col("quality_list_item_pos"), F.col("distinct_site_id_in_rn")).alias("least_quality_list_item_pos_distinct_site_id_in_"),
    )
    .orderBy("quality", "distinct_site_id_in_rn", "quality_list_item_pos")
    .limit(5)
)
