from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date_quality_by_r = Window.partitionBy(F.to_date("read_at"), "quality").orderBy("read_at", "id").rowsBetween(-1, Window.currentRow)
w_read_at_to_date_quality_by_r_2 = Window.partitionBy(F.to_date("read_at"), "quality").orderBy("read_at", "id").rowsBetween(-6, Window.currentRow)
w_latest = Window.partitionBy("zone", "quality").orderBy(F.col("kwh_sum_7").desc(), F.col("id").desc())

in_window = (
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

windowed = (
    in_window
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "zone",
        "rate",
        F.round(F.avg("rate").over(w_read_at_to_date_quality_by_r), 2).alias("rate_avg_2"),
        F.sum("kwh").over(w_read_at_to_date_quality_by_r_2).alias("kwh_sum_7"),
    )
)

deduped = (
    windowed
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "site_id",
        "rate",
        "rate_avg_2",
        "kwh_sum_7",
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.to_date(F.col("read_at")).alias("read_at_date_2"),
    )
    .orderBy(F.col("read_at_date_2").desc(), "id")
    .limit(10)
)
