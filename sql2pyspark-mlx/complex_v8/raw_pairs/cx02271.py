from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_energy_cost_movin = Window.partitionBy("site_id").orderBy(F.col("energy_cost").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_kwh_prior_1 = Window.orderBy("kwh", "id").rowsBetween(-1, -1)
w_site_id_by_energy_cost = Window.partitionBy("site_id").orderBy(F.col("energy_cost").desc(), "id")
w_site_id_by_energy_cost_ties = Window.partitionBy("site_id").orderBy(F.col("energy_cost").desc())
w_latest = Window.partitionBy("site_id").orderBy(F.col("voltage_avg_1_prior").desc(), F.col("id").desc())
w_all_by_energy_cost_rank = Window.orderBy("energy_cost_rank", "id")
w_all_by_energy_cost_rank_ties = Window.orderBy("energy_cost_rank")
w_all_by_energy_cost_rank_movi = Window.orderBy("energy_cost_rank", "id").rowsBetween(-2, Window.currentRow)
w_all_by_energy_cost_percent_r = Window.orderBy("energy_cost_percent_rank", "id")
w_all_by_energy_cost_rank_movi_2 = Window.orderBy("energy_cost_rank", "id").rowsBetween(-6, Window.currentRow)

joined_ranges = (
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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

with_trends = (
    joined_ranges
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "rate",
        F.sum("energy_cost").over(w_site_id_by_energy_cost_movin).alias("energy_cost_sum_2"),
        F.round(F.avg("voltage").over(w_all_by_kwh_prior_1), 2).alias("voltage_avg_1_prior"),
        F.round(F.percent_rank().over(w_site_id_by_energy_cost), 3).alias("energy_cost_percent_rank"),
        F.rank().over(w_site_id_by_energy_cost_ties).alias("energy_cost_rank"),
    )
)

unique_rows = (
    with_trends
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    unique_rows
    .select(
        "*",
        F.ntile(5).over(w_all_by_energy_cost_rank).alias("energy_cost_rank_quintile"),
        F.dense_rank().over(w_all_by_energy_cost_rank_ties).alias("energy_cost_rank_dense_rank"),
        F.max("energy_cost_percent_rank").over(w_all_by_energy_cost_rank_movi).alias("energy_cost_percent_rank_max_3"),
        F.dense_rank().over(w_all_by_energy_cost_percent_r).alias("energy_cost_percent_rank_dense_rank"),
        F.round(F.avg("energy_cost_rank").over(w_all_by_energy_cost_rank_movi_2), 2).alias("energy_cost_rank_avg_7"),
    )
)

result = (
    metrics
    .select(
        "id",
        "site_id",
        "kwh",
        "energy_cost_percent_rank",
        "energy_cost_rank",
        "energy_cost_rank_dense_rank",
        "energy_cost_percent_rank_max_3",
    )
    .orderBy(F.col("energy_cost_rank_dense_rank").desc())
)
