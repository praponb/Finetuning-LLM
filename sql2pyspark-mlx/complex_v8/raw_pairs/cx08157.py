from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at = Window.orderBy("read_at", "id")
w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_ties = Window.orderBy("read_at")

prepared = (
    df_meter_readings
    .filter((F.col("quality").isin("BAD", "OK")) | (F.col("kwh") < 109.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "quality_or_unknown",
        "quality_ref",
        F.ntile(5).over(w_all_by_read_at).alias("read_at_quintile"),
        F.sum("voltage").over(w_site_id_by_voltage_running).alias("running_voltage"),
        F.dense_rank().over(w_all_by_read_at_ties).alias("read_at_dense_rank"),
    )
)

without_matches = (
    with_windows
    .crossJoin(with_windows.agg(F.max("read_at_quintile").alias("overall_max_read_at_quintile")))
    .filter(F.col("read_at_quintile") < F.col("overall_max_read_at_quintile"))
    .drop("overall_max_read_at_quintile")
)

result = (
    without_matches
    .select("id", "read_at_quintile", "read_at_dense_rank")
    .unpivot(
        ids=["id"],
        values=["read_at_quintile", "read_at_dense_rank"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
