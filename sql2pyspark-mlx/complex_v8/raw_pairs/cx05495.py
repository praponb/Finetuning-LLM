from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date_by_kwh = Window.partitionBy(F.to_date("read_at")).orderBy(F.col("kwh").asc_nulls_last(), "id")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id")
w_read_at_to_date_site_id = Window.partitionBy(F.to_date("read_at"), "site_id")
w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy("read_at")
w_site_id = Window.partitionBy("site_id")
w_read_at_year_site_id_by_read = Window.partitionBy(F.year("read_at"), "site_id").orderBy(F.col("read_at").desc(), "id")
w_site_id_by_read_at_2 = Window.partitionBy("site_id").orderBy("read_at", "id")

scoped = (
    df_meter_readings
    .filter(F.col("voltage") >= 218.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.row_number().over(w_read_at_to_date_by_kwh).alias("kwh_seq"),
        F.round(F.cume_dist().over(w_site_id_by_read_at), 3).alias("read_at_cume_dist"),
        F.count(F.lit(1)).over(w_read_at_to_date_site_id).alias("rows_in_read_at_to_date_site_id"),
        F.round(F.percent_rank().over(w_site_id_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.min("voltage").over(w_site_id).alias("site_id_min_voltage"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.round(F.cume_dist().over(w_read_at_year_site_id_by_read), 3).alias("read_at_cume_dist_2"),
        F.lead("kwh", 1, 0).over(w_site_id_by_read_at_2).alias("next_kwh"),
        F.rank().over(w_site_id_by_read_at_ties).alias("read_at_rank"),
        F.lag("voltage", 1, 0).over(w_site_id_by_read_at).alias("prev_voltage"),
        F.lead("kwh", 2).over(w_site_id_by_read_at_2).alias("next2_kwh"),
    )
)

derived = (
    sequenced
    .filter(F.col("read_at_cume_dist") < 0.7)
    .select(
        "*",
        F.year(F.col("read_at")).alias("read_at_year"),
        F.round(F.col("read_at_percent_rank") * 0.9, 2).alias("read_at_percent_rank_adj"),
        F.abs(F.col("voltage") - 232.0).alias("voltage_distance"),
    )
)

result = (
    derived
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_ref",
        "read_at_cume_dist",
        "read_at_percent_rank",
        "kwh_share_of_site_id",
        "next_kwh",
        "read_at_rank",
        "prev_voltage",
        "next2_kwh",
        "read_at_year",
        "read_at_percent_rank_adj",
        "voltage_distance",
    )
    .orderBy("next2_kwh", F.col("kwh_share_of_site_id").desc())
)
