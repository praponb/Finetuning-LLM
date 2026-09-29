from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_qualitys = Window.partitionBy("distinct_qualitys")
w_distinct_qualitys_quality_by = Window.partitionBy("distinct_qualitys", "quality").orderBy("max_voltage", "quality").rowsBetween(-2, Window.currentRow)
w_distinct_qualitys_quality_by_2 = Window.partitionBy("distinct_qualitys", "quality").orderBy("max_voltage", "quality")

prepared_readings = (
    df_meter_readings
    .filter((F.col("voltage") <= 234.5) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

totals = (
    prepared_readings
    .filter(F.col("site_id").isNotNull())
    .groupBy("quality")
    .agg(
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.max("voltage").alias("max_voltage"),
        F.sum("voltage").alias("total_voltage"),
        F.avg("voltage").alias("avg_voltage_check"),
    )
    .filter(F.col("avg_voltage_check") > 215.1)
    .drop("avg_voltage_check")
)

metrics = (
    totals
    .select(
        "quality",
        "distinct_qualitys",
        "max_voltage",
        F.round(F.col("total_voltage") - F.avg("total_voltage").over(w_distinct_qualitys), 2).alias("total_voltage_vs_distinct_qualitys_avg"),
        F.round(F.avg("total_voltage").over(w_distinct_qualitys_quality_by), 2).alias("total_voltage_avg_3"),
        F.row_number().over(w_distinct_qualitys_quality_by_2).alias("max_voltage_seq"),
        F.sum("total_voltage").over(w_distinct_qualitys).alias("distinct_qualitys_sum_total_voltage"),
    )
)

qualified = (
    metrics
    .crossJoin(metrics.agg(F.min("max_voltage_seq").alias("overall_min_max_voltage_seq")))
    .filter(F.col("max_voltage_seq") > F.col("overall_min_max_voltage_seq"))
    .drop("overall_min_max_voltage_seq")
)

leaders = (
    qualified
    .filter(F.col("max_voltage_seq") <= 3)
    .drop("max_voltage_seq")
)

labeled = (
    leaders
    .select(
        "quality",
        "distinct_qualitys",
        "total_voltage_vs_distinct_qualitys_avg",
        "distinct_qualitys_sum_total_voltage",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.round(F.col("max_voltage") / F.nullif(F.col("total_voltage_avg_3"), F.lit(0)), 3).alias("max_voltage_per_total_voltage_avg_3"),
        F.round(F.col("total_voltage_avg_3") * 1.07, 2).alias("total_voltage_avg_3_adj"),
    )
)

result = (
    labeled
    .select(
        "quality",
        "distinct_qualitys",
        "total_voltage_vs_distinct_qualitys_avg",
        "max_voltage_per_total_voltage_avg_3",
        "total_voltage_avg_3_adj",
        F.concat_ws("-", F.col("quality"), F.col("quality_or_unknown")).alias("quality_quality_or_unknown_label"),
        F.round(F.col("total_voltage_avg_3_adj") / F.nullif(F.col("distinct_qualitys"), F.lit(0)), 3).alias("total_voltage_avg_3_adj_per_distinct_qualitys"),
    )
    .orderBy(
        F.col("total_voltage_avg_3_adj_per_distinct_qualitys").desc(),
        "quality",
    )
    .limit(10)
)
