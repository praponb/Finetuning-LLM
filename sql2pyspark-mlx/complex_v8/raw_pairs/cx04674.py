from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_read_at_month_site_id = Window.partitionBy(F.date_trunc("month", "first_read_at"), "site_id").orderBy(F.col("first_read_at").desc(), "site_id", "reading_read_at_month")
w_first_read_at_month_site_id_2 = Window.partitionBy(F.date_trunc("month", "first_read_at"), "site_id").orderBy(F.col("first_read_at").desc())

prepared = (
    df_meter_readings
    .filter((~F.col("quality").isin("OK", "BAD")) & (F.col("voltage") > 218.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

detail_rows = (
    prepared.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

stats = (
    detail_rows
    .groupBy(
        "site_id",
        F.date_trunc("month", "reading_read_at").alias("reading_read_at_month"),
    )
    .agg(
        F.min("reading_read_at").alias("first_reading_read_at"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.min("read_at").alias("first_read_at"),
    )
)

tagged = (
    stats
    .select(
        "site_id",
        "reading_read_at_month",
        "first_read_at",
        F.when(F.col("distinct_site_ids") >= 6, "A").when(F.col("distinct_site_ids") >= 3, "B").otherwise("C").alias("distinct_site_ids_tier"),
    )
)

with_windows = (
    tagged
    .select(
        "site_id",
        "reading_read_at_month",
        "first_read_at",
        "distinct_site_ids_tier",
        F.lead("reading_read_at_month").over(w_first_read_at_month_site_id).alias("next_reading_read_at_month"),
        F.rank().over(w_first_read_at_month_site_id_2).alias("first_read_at_rank"),
        F.round(F.percent_rank().over(w_first_read_at_month_site_id_2), 3).alias("first_read_at_percent_rank"),
        F.round((F.unix_timestamp("reading_read_at_month") - F.unix_timestamp(F.lag("reading_read_at_month").over(w_first_read_at_month_site_id))) / 60, 1).alias("minutes_since_prev_reading_read_at_month"),
        F.round(F.cume_dist().over(w_first_read_at_month_site_id_2), 3).alias("first_read_at_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "site_id",
        "reading_read_at_month",
        "first_read_at_rank",
        "minutes_since_prev_reading_read_at_month",
        "first_read_at_cume_dist",
        F.lower(F.regexp_replace(F.trim(F.col("distinct_site_ids_tier")), " ", "_")).alias("distinct_site_ids_tier_slug"),
    )
)
