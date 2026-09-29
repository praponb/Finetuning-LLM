from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_read_at_t = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at").desc())
w_quality_by_voltage_plus_kwh = Window.partitionBy("quality").orderBy("voltage_plus_kwh", "id").rowsBetween(-1, Window.currentRow)
w_site_id_by_kwh_plus_voltage = Window.partitionBy("site_id").orderBy(F.col("kwh_plus_voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality = Window.partitionBy("quality")
w_site_id_by_kwh_plus_voltage_2 = Window.partitionBy("site_id").orderBy(F.col("kwh_plus_voltage").desc(), "id")
w_kwh_plus_voltage_seq_distinc = Window.partitionBy("kwh_plus_voltage_seq", "distinct_site_id_in_quality").orderBy(F.col("voltage_plus_kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_site_id_in_quality = Window.partitionBy("distinct_site_id_in_quality", "read_at_rank").orderBy("kwh_plus_voltage_seq", "id").rowsBetween(-6, -1)

filtered_readings = (
    df_meter_readings
    .filter((F.col("kwh") >= 16.5) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
        (F.col("kwh") + F.col("voltage")).alias("kwh_plus_voltage"),
        F.greatest(F.col("voltage"), F.col("kwh")).alias("greatest_voltage_kwh"),
    )
)

metrics = (
    filtered_readings
    .select(
        "id",
        "kwh",
        "quality",
        "voltage_plus_kwh",
        "kwh_plus_voltage",
        "greatest_voltage_kwh",
        F.rank().over(w_quality_site_id_by_read_at_t).alias("read_at_rank"),
        F.round(F.avg("greatest_voltage_kwh").over(w_quality_by_voltage_plus_kwh), 2).alias("greatest_voltage_kwh_avg_2"),
        F.max("kwh").over(w_site_id_by_kwh_plus_voltage).alias("max_kwh_so_far"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.round(F.col("voltage_plus_kwh") / F.nullif(F.sum("voltage_plus_kwh").over(w_quality), F.lit(0)), 4).alias("voltage_plus_kwh_share_of_quality"),
        F.row_number().over(w_site_id_by_kwh_plus_voltage_2).alias("kwh_plus_voltage_seq"),
    )
)

filtered = (
    metrics
    .crossJoin(metrics.agg(F.min("kwh").alias("overall_min_kwh")))
    .filter(F.col("kwh") > F.col("overall_min_kwh"))
    .drop("overall_min_kwh")
)

sequenced = (
    filtered
    .select(
        "id",
        "quality",
        "read_at_rank",
        "greatest_voltage_kwh_avg_2",
        "distinct_site_id_in_quality",
        "voltage_plus_kwh_share_of_quality",
        "kwh_plus_voltage_seq",
        F.last("read_at_rank", ignorenulls=True).over(w_kwh_plus_voltage_seq_distinc).alias("last_known_read_at_rank"),
        F.round(F.avg("voltage_plus_kwh").over(w_distinct_site_id_in_quality), 2).alias("voltage_plus_kwh_avg_6_prior"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "read_at_rank",
        "greatest_voltage_kwh_avg_2",
        "voltage_plus_kwh_share_of_quality",
        "last_known_read_at_rank",
        "voltage_plus_kwh_avg_6_prior",
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
    .orderBy(F.col("read_at_rank").asc())
)
