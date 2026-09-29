from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_site_commissioned_on = Window.orderBy(F.col("site_commissioned_on").desc(), "id")
w_site_commissioned_on_year_si = Window.partitionBy(F.year("site_commissioned_on"), "site_id")
w_site_id = Window.partitionBy("site_id")
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy(F.col("voltage").desc_nulls_first(), "id")
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy("voltage")
w_site_id_by_site_commissioned = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").asc_nulls_last(), "id")
w_site_id_by_site_commissioned_2 = Window.partitionBy("site_id").orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_site_commissioned_3 = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_site_commissioned_4 = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").asc_nulls_last())

prepared = (
    df_meter_readings
    .filter(F.col("voltage") >= 219.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.when(F.col("voltage") >= 233.2, "large").when(F.col("voltage") >= 218.2, "medium").otherwise("small").alias("voltage_tier"),
    )
)

joined = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.year(F.col("si.commissioned_on")) == 2025)
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

windowed = (
    joined
    .select(
        "id",
        "site_id",
        "voltage",
        "site_commissioned_on",
        F.round(F.percent_rank().over(w_all_by_site_commissioned_on), 3).alias("site_commissioned_on_percent_rank"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_commissioned_on_year_si), F.lit(0)), 4).alias("voltage_share_of_site_commissioned_on_year_site_"),
        F.sum("voltage").over(w_site_id).alias("site_id_sum_voltage"),
        F.row_number().over(w_all_by_site_commissioned_on).alias("site_commissioned_on_seq"),
        F.rank().over(w_site_id_by_voltage).alias("voltage_rank"),
        F.dense_rank().over(w_site_id_by_voltage_ties).alias("voltage_dense_rank"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_commissioned_on_year_si), 2).alias("voltage_vs_site_commissioned_on_year_site_id_avg"),
        F.ntile(4).over(w_site_id_by_site_commissioned).alias("site_commissioned_on_quartile"),
        F.max("voltage").over(w_site_id_by_site_commissioned_2).alias("max_voltage_so_far"),
        F.last("site_commissioned_on", ignorenulls=True).over(w_site_id_by_site_commissioned_3).alias("last_known_site_commissioned_on"),
        F.dense_rank().over(w_site_id_by_site_commissioned_4).alias("site_commissioned_on_dense_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "site_commissioned_on",
        "site_commissioned_on_percent_rank",
        "voltage_share_of_site_commissioned_on_year_site_",
        "voltage_dense_rank",
        "voltage_vs_site_commissioned_on_year_site_id_avg",
        "last_known_site_commissioned_on",
        "site_commissioned_on_dense_rank",
        F.when(F.col("site_id_sum_voltage") >= 1554.0, "HIGH").when(F.col("site_id_sum_voltage") >= 1106.0, "MEDIUM").otherwise("LOW").alias("site_id_sum_voltage_tier"),
        F.when(F.col("voltage_rank") >= 6, "HIGH").when(F.col("voltage_rank") >= 4, "MEDIUM").otherwise("LOW").alias("voltage_rank_tier"),
    )
    .orderBy(
        F.col("site_commissioned_on").desc(),
        F.col("voltage_vs_site_commissioned_on_year_site_id_avg").desc(),
        "id",
    )
    .limit(20)
)
