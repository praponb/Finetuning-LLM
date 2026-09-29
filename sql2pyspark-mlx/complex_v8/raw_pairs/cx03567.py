from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_by_read_at = Window.partitionBy("zone").orderBy("read_at", "id")
w_zone = Window.partitionBy("zone")
w_quality = Window.partitionBy("quality")
w_site_id_by_seq_no_running = Window.partitionBy("site_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

covered = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

sequenced = (
    covered
    .withColumn("seq_no", F.row_number().over(w_zone_by_read_at))
    .withColumn("distinct_site_id_in_zone", F.size(F.collect_set("site_id").over(w_zone)))
)

with_windows = (
    sequenced
    .select(
        "*",
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality), F.lit(0)), 4).alias("kwh_share_of_quality"),
        F.last("read_at", ignorenulls=True).over(w_site_id_by_seq_no_running).alias("last_known_read_at"),
    )
)

result = (
    with_windows
    .filter(F.col("kwh").between(34.6, 96.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "zone",
        "rate",
        "seq_no",
        "distinct_site_id_in_zone",
        "kwh_share_of_quality",
    )
)
