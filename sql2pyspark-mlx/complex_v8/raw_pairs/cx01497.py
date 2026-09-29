from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_prior_2 = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(-2, -1)

prepared_readings = (
    df_meter_readings
    .filter(F.month(F.col("read_at")).isin(1, 3))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

ranked = (
    prepared_readings
    .select(
        "id",
        "voltage_filled",
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.last("read_at", ignorenulls=True).over(w_all_by_kwh_running).alias("last_known_read_at"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.max("voltage_filled").over(w_all_by_kwh_prior_2).alias("voltage_filled_max_2_prior"),
    )
)

derived = (
    ranked
    .filter(F.col("distinct_quality_in_site_id") >= 5)
    .select(
        "*",
        F.date_format(F.col("last_known_read_at"), "yyyy-MM").alias("last_known_read_at_label"),
        F.when(F.col("voltage_filled") >= 238.0, "HIGH").when(F.col("voltage_filled") >= 220.5, "MEDIUM").otherwise("LOW").alias("voltage_filled_tier"),
        F.abs(F.col("voltage_filled") - 225.8).alias("voltage_filled_distance"),
    )
)

with_activity = (
    derived
    .crossJoin(derived.agg(F.avg("voltage_filled").alias("overall_avg_voltage_filled")))
    .filter(F.col("voltage_filled") > F.col("overall_avg_voltage_filled"))
    .drop("overall_avg_voltage_filled")
)

result = (
    with_activity
    .select(
        "id",
        "voltage_filled_max_2_prior",
        "kwh_share_of_site_id",
        "voltage_filled",
    )
    .unpivot(
        ids=["id"],
        values=["voltage_filled_max_2_prior", "kwh_share_of_site_id", "voltage_filled"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "measure")
)
