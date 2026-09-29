from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_quality_or_unk = Window.partitionBy(F.date_trunc("month", "read_at"), "quality_or_unknown")
w_quality_quality_or_unknown = Window.partitionBy("quality", "quality_or_unknown")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_quality_group_quality_by_vol = Window.partitionBy("quality_group", "quality").orderBy(F.col("voltage").desc(), "id")
w_all_by_voltage_running = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_group_by_voltage = Window.partitionBy("quality_group").orderBy(F.col("voltage").desc_nulls_first(), "id")
w_quality_quality_group_by_rea = Window.partitionBy("quality", "quality_group").orderBy(F.col("read_at").desc(), "id")

cleaned_readings = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.when(F.col("quality").isin("EST", "OK"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

metrics = (
    cleaned_readings
    .select(
        "*",
        F.round(F.avg("voltage").over(w_read_at_month_quality_or_unk), 2).alias("read_at_month_quality_or_unknown_avg_voltage"),
        F.size(F.collect_set("quality_group").over(w_quality_quality_or_unknown)).alias("distinct_quality_group_in_quality_quality_or_unk"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_voltage)).alias("voltage_change"),
        F.lead("voltage", 2).over(w_all_by_voltage).alias("next2_voltage"),
        F.round(F.cume_dist().over(w_quality_group_quality_by_vol), 3).alias("voltage_cume_dist"),
        F.sum("voltage").over(w_all_by_voltage_running).alias("running_voltage"),
        F.lag("voltage").over(w_quality_group_by_voltage).alias("prev_voltage"),
        F.lag("voltage", 2, 0).over(w_quality_quality_group_by_rea).alias("prev2_voltage"),
        F.size(F.collect_set("quality").over(w_read_at_month_quality_or_unk)).alias("distinct_quality_in_read_at_month_quality_or_unk"),
    )
)

enriched = (
    metrics
    .filter(F.col("running_voltage") > 882.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "quality_or_unknown",
        "read_at_month_quality_or_unknown_avg_voltage",
        "distinct_quality_group_in_quality_quality_or_unk",
        "voltage_change",
        "running_voltage",
        "prev_voltage",
        "prev2_voltage",
        "distinct_quality_in_read_at_month_quality_or_unk",
        F.round(F.col("prev_voltage") * 1.1, 2).alias("prev_voltage_adj"),
        F.least(F.col("voltage_change"), F.col("prev2_voltage")).alias("least_voltage_change_prev2_voltage"),
    )
)

result = (
    enriched
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "quality_or_unknown",
        "read_at_month_quality_or_unknown_avg_voltage",
        "distinct_quality_group_in_quality_quality_or_unk",
        "running_voltage",
        "prev_voltage",
        "prev2_voltage",
        "distinct_quality_in_read_at_month_quality_or_unk",
        "prev_voltage_adj",
        "least_voltage_change_prev2_voltage",
    )
    .orderBy(F.col("id").desc())
)
