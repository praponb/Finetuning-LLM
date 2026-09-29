from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_quality = Window.partitionBy(F.year("read_at"), "quality")
w_site_id_read_at_to_date_by_k = Window.partitionBy("site_id", F.to_date("read_at")).orderBy("kwh", "id").rowsBetween(-2, -1)

base_readings = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")) & (F.col("voltage") > 215.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

with_trends = (
    base_readings
    .select(
        "id",
        "read_at",
        "quality",
        "read_at_date",
        "quality_ref",
        F.sum("kwh").over(w_read_at_year_quality).alias("read_at_year_quality_sum_kwh"),
        F.max("kwh").over(w_site_id_read_at_to_date_by_k).alias("kwh_max_2_prior"),
    )
)

result = (
    with_trends
    .groupBy("read_at")
    .pivot("quality", ["EST", "OK", "BAD"])
    .agg(F.avg("read_at_year_quality_sum_kwh"))
    .orderBy("read_at")
)
