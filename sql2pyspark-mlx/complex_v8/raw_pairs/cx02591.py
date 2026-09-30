from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_least_voltage_kwh = Window.partitionBy("quality").orderBy("least_voltage_kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_id = Window.partitionBy("quality", "site_id")

scoped = (
    df_meter_readings
    .filter(F.col("kwh") <= 101.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

scored = (
    scoped
    .select(
        "*",
        F.when(F.col("quality").isin("BAD", "OK"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
    )
)

qualified = (
    scored.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") < 81.1), "left_semi")
)

ranked = (
    qualified
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "quality_group",
        "least_voltage_kwh",
        F.last("voltage").over(w_quality_by_least_voltage_kwh).alias("last_voltage"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
    )
)

result = (
    ranked
    .cube("quality_group", "quality", "site_id")
    .agg(
        F.grouping("quality_group").alias("is_total_quality_group"),
        F.max("read_at").alias("last_read_at"),
    )
    .select(
        F.coalesce(F.col("quality_group"), F.lit("ALL")).alias("quality_group"),
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "site_id",
        "is_total_quality_group",
        "last_read_at",
    )
    .orderBy(
        F.col("quality_group").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
