from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_quality = Window.partitionBy("quality")
w_site_id_quality_by_voltage = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage").desc(), "id")
w_all_by_kwh_ties = Window.orderBy(F.col("kwh").desc())

recent_readings = (
    df_meter_readings
    .filter((F.col("quality") == "EST") & (F.col("site_id") >= 6))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

sequenced = (
    recent_readings
    .select(
        "*",
        F.lead("voltage").over(w_all_by_kwh).alias("next_voltage"),
        F.row_number().over(w_all_by_kwh).alias("kwh_seq"),
        (F.col("voltage") - F.lag("voltage").over(w_site_id_by_kwh)).alias("voltage_change"),
        F.round(F.avg("kwh").over(w_site_id_quality), 2).alias("site_id_quality_avg_kwh"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality), F.lit(0)), 4).alias("voltage_share_of_quality"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id_quality), 2).alias("voltage_vs_site_id_quality_avg"),
        F.round(F.percent_rank().over(w_site_id_quality_by_voltage), 3).alias("voltage_percent_rank"),
        F.dense_rank().over(w_all_by_kwh).alias("kwh_dense_rank"),
        F.dense_rank().over(w_all_by_kwh_ties).alias("kwh_dense_rank_2"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "kwh",
        "quality",
        "next_voltage",
        "kwh_seq",
        "voltage_change",
        "site_id_quality_avg_kwh",
        "voltage_share_of_quality",
        "voltage_vs_site_id_quality_avg",
        "kwh_dense_rank",
        "kwh_dense_rank_2",
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
        (F.col("kwh_seq") + F.col("kwh")).alias("kwh_seq_plus_kwh"),
    )
    .orderBy(F.col("kwh").desc(), "voltage_change", "id")
    .limit(5)
)
