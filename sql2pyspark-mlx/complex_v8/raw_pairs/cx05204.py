from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage_adj = Window.orderBy(F.col("voltage_adj").desc(), "id")
w_quality = Window.partitionBy("quality")
w_site_tariff_quality = Window.partitionBy("site_tariff", "quality")
w_site_tariff_site_id = Window.partitionBy("site_tariff", "site_id")
w_quality_by_kwh = Window.partitionBy("quality").orderBy("kwh", "id")
w_quality_site_tariff_by_read = Window.partitionBy("quality", "site_tariff").orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_site_tariff_quali = Window.partitionBy("site_id").orderBy("site_tariff_quality_max_voltage_adj", "id")
w_all_by_distinct_site_id_in_s = Window.orderBy("distinct_site_id_in_site_tariff_quality", "id")
w_voltage_adj_dense_rank_read = Window.partitionBy("voltage_adj_dense_rank", F.date_trunc("month", "read_at")).orderBy(F.col("min_kwh_so_far").desc(), "id").rowsBetween(-3, -1)
w_voltage_adj_dense_rank_read_2 = Window.partitionBy("voltage_adj_dense_rank", F.date_trunc("month", "read_at")).orderBy(F.col("min_kwh_so_far").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.round(F.col("voltage") * 1.5, 2).alias("voltage_adj"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

joined = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.voltage_adj",
        F.col("si.tariff").alias("site_tariff"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.dense_rank().over(w_all_by_voltage_adj).alias("voltage_adj_dense_rank"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.max("voltage_adj").over(w_site_tariff_quality).alias("site_tariff_quality_max_voltage_adj"),
        F.size(F.collect_set("site_id").over(w_site_tariff_quality)).alias("distinct_site_id_in_site_tariff_quality"),
        F.round(F.avg("kwh").over(w_site_tariff_quality), 2).alias("site_tariff_quality_avg_kwh"),
        F.size(F.collect_set("quality").over(w_site_tariff_site_id)).alias("distinct_quality_in_site_tariff_site_id"),
        F.lead("read_at", 3).over(w_quality_by_kwh).alias("next3_read_at"),
        F.min("kwh").over(w_quality_site_tariff_by_read).alias("min_kwh_so_far"),
        F.sum("voltage_adj").over(w_site_tariff_site_id).alias("site_tariff_site_id_sum_voltage_adj"),
    )
)

labeled = metrics.withColumn("kwh_distance", F.abs(F.col("kwh") - 50.4))

windowed = (
    labeled
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "voltage_adj",
        "voltage_adj_dense_rank",
        "distinct_site_id_in_quality",
        "site_tariff_quality_max_voltage_adj",
        "distinct_site_id_in_site_tariff_quality",
        "site_tariff_quality_avg_kwh",
        "next3_read_at",
        "min_kwh_so_far",
        "site_tariff_site_id_sum_voltage_adj",
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_site_id_by_site_tariff_quali))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.dense_rank().over(w_all_by_distinct_site_id_in_s).alias("distinct_site_id_in_site_tariff_quality_dense_ra"),
        F.round(F.avg("voltage_adj_dense_rank").over(w_voltage_adj_dense_rank_read), 2).alias("voltage_adj_dense_rank_avg_3_prior"),
        F.nth_value("kwh", 2).over(w_voltage_adj_dense_rank_read_2).alias("second_kwh"),
    )
)

result = (
    windowed
    .filter(F.col("minutes_since_prev_read_at") > 1500.0)
    .select(
        "id",
        "site_id",
        "voltage_adj_dense_rank",
        "distinct_site_id_in_quality",
        "site_tariff_quality_avg_kwh",
        "next3_read_at",
        "min_kwh_so_far",
        "site_tariff_site_id_sum_voltage_adj",
        "minutes_since_prev_read_at",
        "distinct_site_id_in_site_tariff_quality_dense_ra",
        "voltage_adj_dense_rank_avg_3_prior",
        "second_kwh",
    )
    .orderBy(F.col("minutes_since_prev_read_at").asc_nulls_last())
)
