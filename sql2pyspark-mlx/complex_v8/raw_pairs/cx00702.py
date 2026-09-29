from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_or_unknown_quality_b = Window.partitionBy("quality_or_unknown", "quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_voltage = Window.partitionBy("quality").orderBy("voltage", "id")
w_quality_read_at_to_date = Window.partitionBy("quality", F.to_date("read_at"))
w_quality_or_unknown_quality = Window.partitionBy("quality_or_unknown", "quality")
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")
w_quality_or_unknown_quality_b_2 = Window.partitionBy("quality_or_unknown", "quality").orderBy("voltage")
w_all_by_voltage_moving_1 = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(-1, Window.currentRow)
w_quality_or_unknown_quality_b_3 = Window.partitionBy("quality_or_unknown", "quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_quality_or_unknown = Window.partitionBy("quality", "quality_or_unknown")
w_quality_or_unknown = Window.partitionBy("quality_or_unknown")
w_first_read_at_month_by_volta = Window.partitionBy(F.date_trunc("month", "first_read_at")).orderBy(F.col("voltage").desc(), "id")
w_voltage_dense_rank_first_rea = Window.partitionBy("voltage_dense_rank", F.to_date("first_read_at")).orderBy(F.col("running_voltage").desc(), "id")

filtered = (
    df_meter_readings
    .filter((F.col("voltage").between(218.4, 238.8)) | (F.col("kwh") <= 110.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.first("read_at").over(w_quality_or_unknown_quality_b).alias("first_read_at"),
        F.round(F.cume_dist().over(w_quality_by_voltage), 3).alias("voltage_cume_dist"),
        F.size(F.collect_set("quality_or_unknown").over(w_quality_read_at_to_date)).alias("distinct_quality_or_unknown_in_quality_read_at_t"),
        F.round(F.avg("voltage").over(w_quality_or_unknown_quality), 2).alias("quality_or_unknown_quality_avg_voltage"),
        F.ntile(4).over(w_all_by_voltage).alias("voltage_quartile"),
        F.size(F.collect_set("site_id").over(w_quality_or_unknown_quality)).alias("distinct_site_id_in_quality_or_unknown_quality"),
        F.dense_rank().over(w_quality_or_unknown_quality_b_2).alias("voltage_dense_rank"),
        F.round(F.avg("voltage").over(w_all_by_voltage_moving_1), 2).alias("voltage_avg_2"),
        F.max("voltage").over(w_quality_read_at_to_date).alias("quality_read_at_to_date_max_voltage"),
        F.sum("voltage").over(w_quality_or_unknown_quality_b_3).alias("running_voltage"),
        F.min("voltage").over(w_quality_quality_or_unknown).alias("quality_quality_or_unknown_min_voltage"),
        F.max("voltage").over(w_quality_or_unknown_quality).alias("quality_or_unknown_quality_max_voltage"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.sum("voltage").over(w_quality_or_unknown).alias("quality_or_unknown_sum_voltage"),
        F.lag("voltage_avg_2", 3, 0).over(w_first_read_at_month_by_volta).alias("prev3_voltage_avg_2"),
        F.lag("voltage_cume_dist", 3).over(w_voltage_dense_rank_first_rea).alias("prev3_voltage_cume_dist"),
        F.min("voltage_dense_rank").over(w_quality_or_unknown).alias("quality_or_unknown_min_voltage_dense_rank"),
        F.round(F.col("voltage_cume_dist") - F.avg("voltage_cume_dist").over(w_quality_or_unknown), 2).alias("voltage_cume_dist_vs_quality_or_unknown_avg"),
    )
)

per_group = (
    ranked
    .filter(F.col("running_voltage").between(770.0, 1666.0))
    .groupBy(
        F.date_format("first_read_at", "yyyy-MM").alias("first_read_at_ym"),
        F.when(F.col("voltage") >= 236.7, "A").when(F.col("voltage") >= 224.8, "B").otherwise("C").alias("voltage_tier"),
    )
    .agg(
        F.max("read_at").alias("last_read_at"),
        F.round(F.sum("prev3_voltage_cume_dist") / F.nullif(F.sum("distinct_quality_or_unknown_in_quality_read_at_t"), F.lit(0)), 3).alias("prev3_voltage_cume_dist_to_distinct_quality_or_u"),
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("n_quality_bad"),
        F.sum("voltage_dense_rank").alias("total_voltage_dense_rank"),
    )
)

result = (
    per_group
    .select(
        "first_read_at_ym",
        "voltage_tier",
        "last_read_at",
        "prev3_voltage_cume_dist_to_distinct_quality_or_u",
        "n_quality_bad",
        "total_voltage_dense_rank",
        F.date_trunc("week", F.col("last_read_at")).alias("last_read_at_week"),
        F.concat_ws(": ", F.col("first_read_at_ym"), F.col("voltage_tier")).alias("first_read_at_ym_voltage_tier_label"),
    )
    .orderBy(F.col("total_voltage_dense_rank").desc())
)
