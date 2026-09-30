from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_quality_group = Window.partitionBy("quality", "quality_group")
w_quality_group_quality_by_vol = Window.partitionBy("quality_group", "quality").orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_group_quality_by_vol_2 = Window.partitionBy("quality_group", "quality").orderBy(F.col("voltage").desc_nulls_first())
w_site_id_quality_by_voltage_a = Window.partitionBy("site_id", "quality").orderBy("voltage_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality_by_voltage_a_2 = Window.partitionBy("site_id", "quality").orderBy("voltage_adj")
w_site_id_quality_by_voltage_a_3 = Window.partitionBy("site_id", "quality").orderBy("voltage_adj", "id").rowsBetween(-1, Window.currentRow)
w_all_by_last_known_voltage = Window.orderBy(F.col("last_known_voltage").desc_nulls_first(), "id")
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy("voltage", "id")

recent = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") * 0.9, 2).alias("voltage_adj"),
        F.when(F.col("quality").isin("BAD", "OK"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
    )
)

derived = (
    recent
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "voltage_adj",
        "quality_group",
        F.coalesce(F.col("voltage_adj"), F.lit(0.0)).alias("voltage_adj_filled"),
    )
)

with_windows = (
    derived
    .select(
        "*",
        F.max("voltage_adj_filled").over(w_quality_quality_group).alias("quality_quality_group_max_voltage_adj_filled"),
        F.last("voltage", ignorenulls=True).over(w_quality_group_quality_by_vol).alias("last_known_voltage"),
        F.round(F.percent_rank().over(w_quality_group_quality_by_vol_2), 3).alias("voltage_percent_rank"),
    )
)

ranked = (
    with_windows
    .select(
        "id",
        "site_id",
        "voltage",
        "voltage_adj",
        "quality_group",
        "voltage_adj_filled",
        "last_known_voltage",
        "voltage_percent_rank",
        F.min("voltage_adj").over(w_site_id_quality_by_voltage_a).alias("min_voltage_adj_so_far"),
        F.round(F.cume_dist().over(w_site_id_quality_by_voltage_a_2), 3).alias("voltage_adj_cume_dist"),
        F.round(F.avg("voltage_adj_filled").over(w_site_id_quality_by_voltage_a_3), 2).alias("voltage_adj_filled_avg_2"),
    )
)

windowed = (
    ranked
    .select(
        "*",
        F.rank().over(w_all_by_last_known_voltage).alias("last_known_voltage_rank"),
        (F.col("last_known_voltage") - F.lag("last_known_voltage").over(w_all_by_last_known_voltage)).alias("last_known_voltage_change"),
        (F.col("voltage_adj_filled_avg_2") - F.lag("voltage_adj_filled_avg_2").over(w_site_id_by_voltage)).alias("voltage_adj_filled_avg_2_change"),
    )
)

result = (
    windowed
    .select(
        "id",
        "voltage",
        "voltage_adj",
        "quality_group",
        "last_known_voltage",
        "min_voltage_adj_so_far",
        "voltage_adj_cume_dist",
        "last_known_voltage_change",
    )
    .orderBy(F.col("quality_group").desc(), F.col("voltage_adj").desc_nulls_first())
)
