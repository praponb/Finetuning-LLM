from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_row_count_site_id_by_row_cou = Window.partitionBy("row_count", "site_id").orderBy("row_count", "site_id", "reading_read_at_day", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id = Window.partitionBy("site_id")
w_row_count_site_id_by_row_cou_2 = Window.partitionBy("row_count", "site_id").orderBy("row_count", "site_id", "reading_read_at_day", "reading_id")

cleaned_readings = (
    df_meter_readings
    .filter(F.col("kwh") > 45.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
        F.abs(F.col("voltage") - 225.4).alias("voltage_distance"),
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

with_children_readings = (
    cleaned_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.read_at_label",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

rollup_base = (
    with_children_readings
    .groupBy("site_id", F.to_date("reading_read_at").alias("reading_read_at_day"))
    .agg(
        F.array_sort(F.collect_set("reading_quality")).alias("reading_quality_list"),
        F.count("*").alias("row_count"),
        F.min("voltage").alias("min_voltage"),
        F.sum("voltage").alias("total_voltage"),
        F.countDistinct("reading_id").alias("distinct_reading_ids"),
    )
    .filter(F.col("row_count") >= 2)
)

exploded_detail_readings = (
    rollup_base.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.site_id",
        "s.reading_read_at_day",
        "s.row_count",
        "s.total_voltage",
        "s.distinct_reading_ids",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
    )
)

qualified = (
    exploded_detail_readings
    .crossJoin(exploded_detail_readings.agg(F.avg("total_voltage").alias("overall_avg_total_voltage")))
    .filter(F.col("total_voltage") > F.col("overall_avg_total_voltage"))
    .drop("overall_avg_total_voltage")
)

metrics = (
    qualified
    .select(
        "*",
        F.first("total_voltage").over(w_row_count_site_id_by_row_cou).alias("first_total_voltage"),
        F.round(F.col("total_voltage") - F.avg("total_voltage").over(w_site_id), 2).alias("total_voltage_vs_site_id_avg"),
        F.sum("reading_kwh").over(w_site_id).alias("site_id_sum_reading_kwh"),
        F.row_number().over(w_row_count_site_id_by_row_cou_2).alias("seq_no"),
        F.rank().over(w_row_count_site_id_by_row_cou_2).alias("row_count_rank"),
        F.nth_value("distinct_reading_ids", 2).over(w_row_count_site_id_by_row_cou).alias("second_distinct_reading_ids"),
    )
)

result = (
    metrics
    .select(
        "site_id",
        "reading_read_at_day",
        "site_id_sum_reading_kwh",
        "total_voltage",
        "reading_kwh",
    )
    .unpivot(
        ids=["site_id", "reading_read_at_day"],
        values=["site_id_sum_reading_kwh", "total_voltage", "reading_kwh"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
