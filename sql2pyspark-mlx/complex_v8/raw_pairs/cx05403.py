from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_quality_or_unknown_site_id = Window.partitionBy("quality_or_unknown", "site_id")
w_quality_or_unknown_read_at_t = Window.partitionBy("quality_or_unknown", F.to_date("read_at")).orderBy(F.col("voltage").desc_nulls_first())
w_quality_or_unknown_read_at_t_2 = Window.partitionBy("quality_or_unknown", F.to_date("read_at")).orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id")
w_quality_or_unknown_read_at_t_3 = Window.partitionBy("quality_or_unknown", F.to_date("read_at")).orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_read_at_full = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_read_at_year_by_kwh = Window.partitionBy("site_id", F.year("read_at")).orderBy(F.col("kwh").asc_nulls_last(), "id")
w_quality_or_unknown_by_voltag = Window.partitionBy("quality_or_unknown").orderBy("voltage")
w_quality_or_unknown_read_at_t_4 = Window.partitionBy("quality_or_unknown", F.to_date("read_at")).orderBy(F.col("voltage").desc_nulls_first(), "id")
w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy(F.col("read_at").desc())

base = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(7, 10)) & (F.col("quality").isin("EST", "BAD")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

sequenced = (
    base
    .select(
        "*",
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_or_unknown_site_id), 2).alias("voltage_vs_quality_or_unknown_site_id_avg"),
        F.dense_rank().over(w_quality_or_unknown_read_at_t).alias("voltage_dense_rank"),
        F.round(F.avg("kwh").over(w_quality_or_unknown_read_at_t_2), 2).alias("kwh_avg_7"),
        F.round(F.percent_rank().over(w_site_id_by_read_at), 3).alias("read_at_percent_rank"),
        F.size(F.collect_set("quality_or_unknown").over(w_site_id)).alias("distinct_quality_or_unknown_in_site_id"),
        F.max("kwh").over(w_quality_or_unknown_site_id).alias("quality_or_unknown_site_id_max_kwh"),
        F.first("read_at", ignorenulls=True).over(w_quality_or_unknown_read_at_t_3).alias("first_known_read_at"),
        F.nth_value("kwh", 2).over(w_site_id_by_read_at_full).alias("second_kwh"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
        F.lag("voltage").over(w_site_id_read_at_year_by_kwh).alias("prev_voltage"),
        F.min("kwh").over(w_site_id).alias("site_id_min_kwh"),
        F.dense_rank().over(w_quality_or_unknown_by_voltag).alias("voltage_dense_rank_2"),
        F.row_number().over(w_quality_or_unknown_read_at_t_4).alias("voltage_seq"),
        F.dense_rank().over(w_site_id_by_read_at_ties).alias("read_at_dense_rank"),
    )
)

leaders = sequenced.filter(F.col("voltage_dense_rank") <= 5)

result = (
    leaders
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_or_unknown",
        "kwh_vs_site_id_avg",
        "voltage_vs_quality_or_unknown_site_id_avg",
        "voltage_dense_rank",
        "kwh_avg_7",
        "read_at_percent_rank",
        "first_known_read_at",
        "prev_voltage",
        "read_at_dense_rank",
        F.month(F.col("read_at")).alias("read_at_month"),
    )
    .orderBy(F.col("id").desc(), "read_at_dense_rank")
)
