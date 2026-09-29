from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), F.col("id").desc())
w_site_id_read_at_to_date_by_k = Window.partitionBy("site_id", F.to_date("read_at")).orderBy("kwh", "id").rowsBetween(-6, Window.currentRow)
w_site_tariff_by_voltage = Window.partitionBy("site_tariff").orderBy(F.col("voltage").desc_nulls_first(), "id")

filtered = (
    df_meter_readings
    .filter((F.col("quality") != "BAD") & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

unique_rows = (
    filtered
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    unique_rows.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") <= 386.6), "inner")
    .select("s.*", F.col("si.tariff").alias("site_tariff"))
)

sequenced = (
    joined
    .select(
        "*",
        F.round(F.avg("voltage").over(w_site_id_read_at_to_date_by_k), 2).alias("voltage_avg_7"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_site_tariff_by_voltage))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.ntile(5).over(w_site_tariff_by_voltage).alias("voltage_quintile"),
    )
)

combined_details = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") <= 446.9), "inner")
    .select("s.*", F.col("si.zone").alias("site_zone"))
)

result = (
    combined_details
    .groupBy("site_id")
    .pivot("quality", ["EST", "BAD", "OK"])
    .agg(F.avg("kwh"))
    .orderBy("site_id")
)
