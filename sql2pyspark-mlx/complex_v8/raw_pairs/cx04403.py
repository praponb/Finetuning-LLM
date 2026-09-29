from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")
w_site_zone_by_kwh = Window.partitionBy("site_zone").orderBy(F.col("kwh").asc_nulls_last(), "id")
w_site_zone_site_id = Window.partitionBy("site_zone", "site_id")

prepared = (
    df_meter_readings
    .filter((F.col("voltage") > 215.1) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

combined_details = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

with_trends = (
    combined_details
    .select(
        "id",
        "site_id",
        "read_at",
        "site_commissioned_on",
        F.row_number().over(w_site_id_by_kwh).alias("kwh_seq"),
        F.row_number().over(w_site_zone_by_kwh).alias("seq_no"),
        F.sum("kwh").over(w_site_zone_site_id).alias("site_zone_site_id_sum_kwh"),
        F.min("kwh").over(w_site_zone_site_id).alias("site_zone_site_id_min_kwh"),
        F.max("kwh").over(w_site_zone_site_id).alias("site_zone_site_id_max_kwh"),
    )
)

enriched = (
    with_trends
    .withColumn("kwh_seq_per_seq_no", F.round(F.col("kwh_seq") / F.nullif(F.col("seq_no"), F.lit(0)), 3))
)

annotated = (
    enriched
    .filter(F.col("site_id").isNotNull())
    .select(
        "*",
        (F.col("read_at") + F.expr("INTERVAL 15 MINUTES")).alias("read_at_plus_15m"),
        (F.col("read_at") + F.expr("INTERVAL 7 DAYS")).alias("read_at_plus_7d"),
    )
)

scored = (
    annotated
    .filter(F.hour(F.col("read_at_plus_15m")).between(8, 11))
    .select(
        "*",
        F.round(F.col("kwh_seq_per_seq_no") / F.nullif(F.col("seq_no"), F.lit(0)), 3).alias("kwh_seq_per_seq_no_per_seq_no"),
        F.date_add(F.col("site_commissioned_on"), 30).alias("site_commissioned_on_plus_30d"),
        F.least(F.col("kwh_seq"), F.col("kwh_seq_per_seq_no")).alias("least_kwh_seq_kwh_seq_per_seq_no"),
    )
)

result = (
    scored
    .select(
        "id",
        "site_id",
        "read_at",
        "site_commissioned_on",
        "kwh_seq",
        "seq_no",
        "site_zone_site_id_min_kwh",
        "kwh_seq_per_seq_no",
        "read_at_plus_15m",
        "read_at_plus_7d",
        "least_kwh_seq_kwh_seq_per_seq_no",
        F.date_format(F.col("read_at_plus_7d"), "yyyy-MM-dd").alias("read_at_plus_7d_label"),
        (F.col("read_at_plus_15m") + F.expr("INTERVAL 2 HOURS")).alias("read_at_plus_15m_plus_2h"),
    )
    .orderBy(
        F.col("read_at_plus_15m_plus_2h").desc(),
        F.col("read_at").asc_nulls_last(),
    )
)
