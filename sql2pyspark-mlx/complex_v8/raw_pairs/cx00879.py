from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_kwh_ties = Window.partitionBy("site_id").orderBy("kwh")
w_site_id = Window.partitionBy("site_id")
w_site_id_quality_by_kwh = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").desc(), "id")
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")

base = (
    df_meter_readings
    .filter(F.col("kwh") >= 33.9)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.month(F.col("read_at")).alias("read_at_month"),
    )
)

qualified = (
    base.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")), "left_anti")
)

ranked = (
    qualified
    .select(
        "*",
        F.rank().over(w_site_id_by_kwh_ties).alias("kwh_rank"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.row_number().over(w_site_id_quality_by_kwh).alias("kwh_seq"),
        F.lead("read_at", 2).over(w_site_id_by_kwh).alias("next2_read_at"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.count("*").alias("reading_count"),
    )
)

with_stats = (
    ranked.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    with_stats
    .filter(F.col("quality") == "OK")
    .select(
        "id",
        "site_id",
        "quality",
        "read_at_month",
        "rows_in_site_id",
        "kwh_seq",
        "next2_read_at",
        "last_reading_read_at",
        "reading_count",
        F.date_trunc("week", F.col("next2_read_at")).alias("next2_read_at_week"),
        F.date_trunc("quarter", F.col("last_reading_read_at")).alias("last_reading_read_at_quarter"),
    )
    .orderBy(F.col("rows_in_site_id").desc_nulls_first())
)
