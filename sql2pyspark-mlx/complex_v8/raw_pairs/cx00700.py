from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_read_at_year_by_kwh = Window.partitionBy("quality", F.year("read_at")).orderBy(F.col("kwh").desc(), "id").rowsBetween(-2, Window.currentRow)
w_read_at_to_date_quality_by_r = Window.partitionBy(F.to_date("read_at"), "quality").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_to_date_quality_by_r_2 = Window.partitionBy(F.to_date("read_at"), "quality").orderBy(F.col("read_at").desc())
w_read_at_to_date_quality_by_r_3 = Window.partitionBy(F.to_date("read_at"), "quality").orderBy(F.col("read_at").desc(), "id")
w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

metrics = (
    covered
    .select(
        "*",
        F.round(F.avg("kwh").over(w_quality_read_at_year_by_kwh), 2).alias("kwh_avg_3"),
        F.sum("energy_cost").over(w_read_at_to_date_quality_by_r).alias("running_energy_cost"),
        F.rank().over(w_read_at_to_date_quality_by_r_2).alias("read_at_rank"),
        F.row_number().over(w_read_at_to_date_quality_by_r_3).alias("rn"),
        F.first("kwh", ignorenulls=True).over(w_all_by_read_at_running).alias("first_known_kwh"),
    )
)

top_per_group = (
    metrics
    .filter(F.col("read_at_rank") <= 2)
    .drop("read_at_rank")
)

result = (
    top_per_group
    .filter(F.col("rate") <= 5.7)
    .select(
        "id",
        "site_id",
        "quality",
        "zone",
        "rate",
        "rn",
        "first_known_kwh",
        F.abs(F.col("kwh_avg_3") - 42.0).alias("kwh_avg_3_distance"),
        F.when(F.col("zone").isin("Z4", "Z3"), "group_a").when(F.col("zone") == "Z2", "group_b").otherwise("other").alias("zone_group"),
    )
    .orderBy(F.col("first_known_kwh").desc(), F.col("zone").desc())
)
