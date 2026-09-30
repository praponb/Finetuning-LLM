from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_voltage_m = Window.partitionBy("quality", "site_id").orderBy("voltage", "id").rowsBetween(-2, Window.currentRow)
w_quality_site_id_by_voltage_p = Window.partitionBy("quality", "site_id").orderBy("voltage", "id").rowsBetween(-1, -1)

base = (
    df_meter_readings
    .filter(F.col("kwh") >= 45.3)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

with_windows = (
    base
    .withColumn("voltage_max_3", F.max("voltage").over(w_quality_site_id_by_voltage_m))
    .withColumn("voltage_sum_1_prior", F.sum("voltage").over(w_quality_site_id_by_voltage_p))
)

filtered = (
    with_windows
    .crossJoin(with_windows.agg(F.avg("kwh").alias("overall_avg_kwh")))
    .filter(F.col("kwh") > F.col("overall_avg_kwh"))
    .drop("overall_avg_kwh")
)

enriched = (
    filtered
    .select(
        "*",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

result = (
    enriched
    .select(
        "id",
        F.col("voltage_minus_kwh").alias("m_voltage_minus_kwh"),
        F.col("kwh").alias("kwh_v"),
        F.col("voltage").alias("m_voltage"),
    )
    .unpivot(
        ids=["id"],
        values=["m_voltage_minus_kwh", "kwh_v", "m_voltage"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric_name")
)
