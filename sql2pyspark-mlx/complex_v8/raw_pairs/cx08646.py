from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone = Window.partitionBy("zone")
w_site_id_zone_by_read_at_runn = Window.partitionBy("site_id", "zone").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_ties = Window.orderBy("read_at")
w_site_id_zone_by_read_at = Window.partitionBy("site_id", "zone").orderBy("read_at", "id")
w_read_at_to_date_zone = Window.partitionBy(F.to_date("read_at"), "zone")
w_site_id_read_at_dense_rank_b = Window.partitionBy("site_id", "read_at_dense_rank").orderBy(F.col("read_at_seq").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

in_window = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
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
        "*",
        F.min("energy_cost").over(w_zone).alias("zone_min_energy_cost"),
        F.max("energy_cost").over(w_site_id_zone_by_read_at_runn).alias("max_energy_cost_so_far"),
        F.dense_rank().over(w_all_by_read_at_ties).alias("read_at_dense_rank"),
        F.row_number().over(w_site_id_zone_by_read_at).alias("read_at_seq"),
    )
)

sequenced = (
    windowed
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "zone",
        "energy_cost",
        "zone_min_energy_cost",
        "max_energy_cost_so_far",
        "read_at_dense_rank",
        F.size(F.collect_set("site_id").over(w_read_at_to_date_zone)).alias("distinct_site_id_in_read_at_to_date_zone"),
        F.count(F.lit(1)).over(w_site_id_read_at_dense_rank_b).alias("running_count"),
        F.count(F.lit(1)).over(w_read_at_to_date_zone).alias("rows_in_read_at_to_date_zone"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.count("*").alias("reading_count"),
    )
)

summarized_rows = (
    sequenced.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.first_reading_read_at",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    summarized_rows
    .filter(F.hour(F.col("first_reading_read_at")).between(8, 13))
    .select(
        "id",
        "quality",
        "zone",
        "energy_cost",
        "max_energy_cost_so_far",
        "read_at_dense_rank",
        "distinct_site_id_in_read_at_to_date_zone",
        "running_count",
        "first_reading_read_at",
        "reading_count",
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
    .orderBy(F.col("quality_group").asc())
)
