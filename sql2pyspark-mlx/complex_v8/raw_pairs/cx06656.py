from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_quality_by_energy_cost_runni = Window.partitionBy("quality").orderBy("energy_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_dedup = Window.partitionBy("zone").orderBy(F.col("read_at").desc(), F.col("id").desc())
w_all_by_running_count_ties = Window.orderBy(F.col("running_count").desc())

matched = (
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

with_trends = (
    matched
    .withColumn("rows_in_site_id", F.count(F.lit(1)).over(w_site_id))
    .withColumn("running_count", F.count(F.lit(1)).over(w_quality_by_energy_cost_runni))
)

latest_per_key = (
    with_trends
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

metrics = (
    latest_per_key
    .select(
        "*",
        F.round(F.avg("rate").over(w_site_id), 2).alias("site_id_avg_rate"),
        F.dense_rank().over(w_all_by_running_count_ties).alias("running_count_dense_rank"),
    )
)

result = (
    metrics
    .groupingSets(
        [["running_count_dense_rank", "zone", "quality"], ["running_count_dense_rank"], ["zone"], []],
        "running_count_dense_rank", "zone", "quality",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("read_at").alias("first_read_at"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
    )
    .select(
        "running_count_dense_rank",
        F.coalesce(F.col("zone"), F.lit("ALL")).alias("zone"),
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "gid",
        "first_read_at",
        "quality_list",
    )
    .orderBy(
        F.col("running_count_dense_rank").asc_nulls_last(),
        F.col("zone").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
