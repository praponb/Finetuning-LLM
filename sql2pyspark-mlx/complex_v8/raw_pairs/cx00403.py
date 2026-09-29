from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_month_by_kwh = Window.partitionBy(F.date_trunc("month", "read_at_year")).orderBy("kwh", "id")
w_all_by_read_at = Window.orderBy("read_at", "id")
w_site_zone_by_kwh = Window.partitionBy("site_zone").orderBy(F.col("kwh").desc(), "id")
w_site_zone = Window.partitionBy("site_zone")

cleaned_readings = (
    df_meter_readings
    .filter((F.col("kwh") <= 81.3) & (F.col("voltage").between(219.6, 243.1)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
    )
)

sequenced = (
    cleaned_readings
    .withColumn("next_read_at", F.lead("read_at").over(w_read_at_year_month_by_kwh))
    .withColumn("read_at_decile", F.ntile(10).over(w_all_by_read_at))
)

with_parent = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on").isNotNull()), "inner")
    .select("s.*", "si.site_name", F.col("si.zone").alias("site_zone"))
)

sequenced_2 = (
    with_parent
    .select(
        "*",
        (F.col("kwh") - F.lag("kwh").over(w_site_zone_by_kwh)).alias("kwh_change"),
        F.lag("kwh").over(w_site_zone_by_kwh).alias("prev_kwh"),
        F.sum("voltage").over(w_site_zone).alias("site_zone_sum_voltage"),
        F.dense_rank().over(w_site_zone_by_kwh).alias("kwh_dense_rank"),
        F.min("read_at_decile").over(w_site_zone).alias("site_zone_min_read_at_decile"),
    )
)

result = (
    sequenced_2
    .groupBy("read_at_decile", "site_id")
    .pivot("site_zone", ["Z4", "Z3", "Z2"])
    .agg(F.avg("site_zone_sum_voltage"))
    .orderBy("read_at_decile", "site_id")
)
