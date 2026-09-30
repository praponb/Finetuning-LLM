from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_read_at_month_by_energy = Window.partitionBy("zone", F.month("read_at")).orderBy("energy_cost", "id")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_zone_read_at_month_by_energy_2 = Window.partitionBy("zone", F.month("read_at")).orderBy("energy_cost")
w_zone_read_at_month_by_energy_3 = Window.partitionBy("zone", F.month("read_at")).orderBy("energy_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("quality").orderBy(F.col("energy_cost").desc(), F.col("id").desc())

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

ranked = (
    covered
    .select(
        "id",
        "kwh",
        "quality",
        "zone",
        "rate",
        "energy_cost",
        F.row_number().over(w_zone_read_at_month_by_energy).alias("seq_no"),
        F.round(F.col("energy_cost") / F.nullif(F.sum("energy_cost").over(w_quality_site_id), F.lit(0)), 4).alias("energy_cost_share_of_quality_site_id"),
        F.rank().over(w_zone_read_at_month_by_energy_2).alias("energy_cost_rank"),
        F.sum("energy_cost").over(w_zone_read_at_month_by_energy_3).alias("running_energy_cost"),
    )
)

latest = (
    ranked
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .rollup("energy_cost_rank", "quality")
    .agg(
        F.grouping("energy_cost_rank").alias("is_total_energy_cost_rank"),
        F.round(F.avg("energy_cost_rank"), 2).alias("avg_energy_cost_rank"),
    )
    .orderBy(
        F.col("energy_cost_rank").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
