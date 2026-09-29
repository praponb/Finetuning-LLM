from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_read_at_year_by_voltage_to_k = Window.partitionBy("read_at_year").orderBy(F.col("voltage_to_kwh_ratio").desc(), "read_at_year", "quality", "site_id").rowsBetween(-3, Window.currentRow)
w_read_at_year_site_id_by_volt = Window.partitionBy("read_at_year", "site_id").orderBy(F.col("voltage_to_kwh_ratio").desc_nulls_first(), "read_at_year", "quality", "site_id")

valid_readings = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp")) & (F.col("kwh") > 48.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

summary = (
    valid_readings
    .groupBy(F.year("read_at").alias("read_at_year"), "quality", "site_id")
    .agg(
        F.min("voltage").alias("min_voltage"),
        F.round(F.sum("voltage") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("voltage_to_kwh_ratio"),
    )
)

ranked = (
    summary
    .select(
        "read_at_year",
        "quality",
        "site_id",
        "min_voltage",
        "voltage_to_kwh_ratio",
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.round(F.avg("read_at_year").over(w_read_at_year_by_voltage_to_k), 2).alias("read_at_year_avg_4"),
        F.dense_rank().over(w_read_at_year_site_id_by_volt).alias("voltage_to_kwh_ratio_dense_rank"),
    )
)

result = (
    ranked
    .select(
        "read_at_year",
        "site_id",
        "min_voltage",
        "distinct_site_id_in_quality",
        "read_at_year_avg_4",
        "voltage_to_kwh_ratio_dense_rank",
        F.round(F.col("voltage_to_kwh_ratio") * 1.07, 2).alias("voltage_to_kwh_ratio_adj"),
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
    .orderBy(F.col("voltage_to_kwh_ratio_adj").desc())
)
