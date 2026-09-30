from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_ties = Window.orderBy(F.col("read_at").desc())
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_all_by_read_at_full = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage = Window.orderBy("voltage", "id")
w_dedup = Window.partitionBy("site_id", "quality").orderBy("prev2_read_at", "id")

prepared_readings = (
    df_meter_readings
    .filter((F.col("voltage") < 240.2) & (F.col("quality").isin("OK", "BAD")))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

enriched = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-11-28").cast("date"))), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "si.site_name",
    )
)

metrics = (
    enriched
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        F.round(F.cume_dist().over(w_all_by_read_at_ties), 3).alias("read_at_cume_dist"),
        F.lag("read_at", 2).over(w_site_id_by_read_at).alias("prev2_read_at"),
        F.lag("read_at").over(w_all_by_read_at).alias("prev_read_at"),
        F.last("read_at").over(w_all_by_read_at_full).alias("last_read_at"),
        F.row_number().over(w_all_by_voltage).alias("rn"),
    )
)

filtered = (
    metrics.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") <= 232.4), "left_anti")
)

deduped = (
    filtered
    .withColumn("rn_2", F.row_number().over(w_dedup))
    .filter(F.col("rn_2") == 1)
    .drop("rn_2")
)

result = (
    deduped
    .rollup("quality", "site_id")
    .agg(
        F.sum("read_at_cume_dist").alias("total_read_at_cume_dist"),
        F.count("*").alias("cnt"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "site_id",
        "total_read_at_cume_dist",
        "cnt",
    )
)
