from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_site_id = Window.partitionBy(F.year("read_at"), "site_id")
w_all_by_read_at_ties = Window.orderBy("read_at")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id")
w_read_at_to_date_site_id_by_v = Window.partitionBy(F.to_date("read_at"), "site_id").orderBy(F.col("voltage").desc(), "id")
w_read_at_to_date_site_id_by_v_2 = Window.partitionBy(F.to_date("read_at"), "site_id").orderBy(F.col("voltage").desc(), "id").rowsBetween(-2, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

sequenced = (
    scoped_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        F.sum("kwh").over(w_read_at_year_site_id).alias("read_at_year_site_id_sum_kwh"),
        F.dense_rank().over(w_all_by_read_at_ties).alias("read_at_dense_rank"),
        F.rank().over(w_site_id_by_read_at).alias("read_at_rank"),
        F.round(F.cume_dist().over(w_read_at_to_date_site_id_by_v), 3).alias("voltage_cume_dist"),
        F.round(F.avg("voltage").over(w_read_at_to_date_site_id_by_v_2), 2).alias("voltage_avg_3"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_year_site_id), 2).alias("kwh_vs_read_at_year_site_id_avg"),
    )
)

first_rows = (
    sequenced
    .filter(F.col("read_at_rank") <= 3)
    .drop("read_at_rank")
)

result = (
    first_rows
    .filter(F.col("kwh") <= 80.8)
    .select(
        "id",
        "kwh",
        "voltage",
        "voltage_cume_dist",
        "voltage_avg_3",
        "kwh_vs_read_at_year_site_id_avg",
        F.round(F.col("voltage_cume_dist") * 1.07, 2).alias("voltage_cume_dist_adj"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
