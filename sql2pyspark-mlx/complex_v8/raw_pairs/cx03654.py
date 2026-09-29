from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_by_greatest_vo = Window.partitionBy("read_at_month").orderBy(F.col("greatest_voltage_kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month_quality_ref_le = Window.partitionBy("read_at_month", "quality_ref_len")
w_quality_ref_len_read_at_mont = Window.partitionBy("quality_ref_len", "read_at_month")
w_read_at_month_quality_ref_le_2 = Window.partitionBy("read_at_month", "quality_ref_len").orderBy(F.col("read_at_month").asc_nulls_last(), "id")

valid = (
    df_meter_readings
    .filter(F.col("site_id") >= 3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
        F.greatest(F.col("voltage"), F.col("kwh")).alias("greatest_voltage_kwh"),
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
    )
)

tagged = (
    valid
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "quality_ref",
        "greatest_voltage_kwh",
        "voltage_minus_kwh",
        F.concat_ws("|", F.col("quality_ref"), F.col("quality")).alias("quality_ref_quality_label"),
        F.month(F.col("read_at")).alias("read_at_month"),
        F.initcap(F.trim(F.col("quality_ref"))).alias("quality_ref_clean"),
    )
)

enriched = (
    tagged
    .filter(F.col("voltage_minus_kwh") >= 54.8)
    .select(
        "id",
        "greatest_voltage_kwh",
        "read_at_month",
        "quality_ref_clean",
        F.length(F.trim(F.col("quality_ref"))).alias("quality_ref_len"),
        F.round(F.col("greatest_voltage_kwh") * 1.5, 2).alias("greatest_voltage_kwh_adj"),
        F.round(F.col("greatest_voltage_kwh") / F.nullif(F.col("voltage_minus_kwh"), F.lit(0)), 3).alias("greatest_voltage_kwh_per_voltage_minus_kwh"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "greatest_voltage_kwh",
        "read_at_month",
        "quality_ref_clean",
        "quality_ref_len",
        "greatest_voltage_kwh_per_voltage_minus_kwh",
        F.first("greatest_voltage_kwh", ignorenulls=True).over(w_read_at_month_by_greatest_vo).alias("first_known_greatest_voltage_kwh"),
        F.max("read_at_month").over(w_read_at_month_quality_ref_le).alias("read_at_month_quality_ref_len_max_read_at_month"),
    )
)

filtered = (
    metrics
    .crossJoin(metrics.agg(F.avg("read_at_month").alias("overall_avg_read_at_month")))
    .filter(F.col("read_at_month") > F.col("overall_avg_read_at_month"))
    .drop("overall_avg_read_at_month")
)

with_trends = (
    filtered
    .select(
        "id",
        "read_at_month",
        "quality_ref_clean",
        "greatest_voltage_kwh_per_voltage_minus_kwh",
        "read_at_month_quality_ref_len_max_read_at_month",
        F.sum("greatest_voltage_kwh").over(w_quality_ref_len_read_at_mont).alias("quality_ref_len_read_at_month_sum_greatest_volta"),
        F.round(F.avg("quality_ref_len").over(w_quality_ref_len_read_at_mont), 2).alias("quality_ref_len_read_at_month_avg_quality_ref_le"),
        F.round(F.col("quality_ref_len") / F.nullif(F.sum("quality_ref_len").over(w_quality_ref_len_read_at_mont), F.lit(0)), 4).alias("quality_ref_len_share_of_quality_ref_len_read_at"),
        F.round(F.percent_rank().over(w_read_at_month_quality_ref_le_2), 3).alias("read_at_month_percent_rank"),
        (F.col("read_at_month") - F.lag("read_at_month").over(w_read_at_month_quality_ref_le_2)).alias("read_at_month_change"),
        F.sum("quality_ref_len").over(w_quality_ref_len_read_at_mont).alias("quality_ref_len_read_at_month_sum_quality_ref_le"),
    )
)

with_activity = (
    with_trends
    .crossJoin(with_trends.agg(F.max("greatest_voltage_kwh_per_voltage_minus_kwh").alias("overall_max_greatest_voltage_kwh_per_voltage_min")))
    .filter(F.col("greatest_voltage_kwh_per_voltage_minus_kwh") < F.col("overall_max_greatest_voltage_kwh_per_voltage_min"))
    .drop("overall_max_greatest_voltage_kwh_per_voltage_min")
)

result = (
    with_activity
    .select(
        "id",
        "read_at_month",
        "greatest_voltage_kwh_per_voltage_minus_kwh",
        "read_at_month_quality_ref_len_max_read_at_month",
        "quality_ref_len_read_at_month_sum_greatest_volta",
        "quality_ref_len_share_of_quality_ref_len_read_at",
        "read_at_month_percent_rank",
        "read_at_month_change",
        "quality_ref_len_read_at_month_sum_quality_ref_le",
        F.lower(F.regexp_replace(F.trim(F.col("quality_ref_clean")), " ", "_")).alias("quality_ref_clean_slug"),
        (F.col("read_at_month_change") - F.col("read_at_month")).alias("read_at_month_change_minus_read_at_month"),
    )
    .orderBy(F.col("read_at_month_change").asc())
)
