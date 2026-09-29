from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_site_id_by_rea = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id").orderBy(F.col("read_at").desc(), "id")
w_quality_site_id_by_kwh_full = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_quality = Window.partitionBy(F.date_trunc("month", "read_at"), "quality")
w_quality_read_at_month = Window.partitionBy("quality", F.date_trunc("month", "read_at"))
w_quality_site_id_by_kwh_movin = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(-3, Window.currentRow)
w_site_id_by_kwh_ties = Window.partitionBy("site_id").orderBy("kwh")
w_quality_site_id_by_kwh = Window.partitionBy("quality", "site_id").orderBy("kwh", "id")
w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_site_id_by_rea_2 = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_id_by_kwh_2 = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc(), "id")
w_read_at_seq_quality_by_secon = Window.partitionBy("read_at_seq", "quality").orderBy("second_voltage_2", "id")
w_read_at_seq = Window.partitionBy("read_at_seq")
w_read_at_seq_quality_by_secon_2 = Window.partitionBy("read_at_seq", "quality").orderBy("second_voltage_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_seq_by_avg_next_volt = Window.partitionBy("read_at_seq").orderBy("avg_next_voltage", "seq_no", "read_at_seq").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_read_at_month_read_at = Window.partitionBy(F.month("first_read_at"), "read_at_seq").orderBy(F.col("avg_next_voltage").desc(), "seq_no", "read_at_seq").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_seq_by_avg_next_volt_2 = Window.partitionBy("read_at_seq").orderBy("avg_next_voltage", "seq_no", "read_at_seq")
w_first_read_at_month_read_at_2 = Window.partitionBy(F.month("first_read_at"), "read_at_seq").orderBy(F.col("avg_next_voltage").desc(), "seq_no", "read_at_seq")

scoped = (
    df_meter_readings
    .filter((F.col("voltage") >= 215.6) & (F.to_date(F.col("read_at")) < F.lit("2025-02-06").cast("date")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.lag("kwh", 3, 0).over(w_read_at_month_site_id_by_rea).alias("prev3_kwh"),
        F.nth_value("voltage", 2).over(w_quality_site_id_by_kwh_full).alias("second_voltage"),
        F.row_number().over(w_read_at_month_site_id_by_rea).alias("read_at_seq"),
        F.count(F.lit(1)).over(w_read_at_month_quality).alias("rows_in_read_at_month_quality"),
        F.max("kwh").over(w_read_at_month_quality).alias("read_at_month_quality_max_kwh"),
        F.sum("kwh").over(w_quality_read_at_month).alias("quality_read_at_month_sum_kwh"),
        F.round(F.avg("voltage").over(w_quality_site_id_by_kwh_movin), 2).alias("voltage_avg_4"),
        F.rank().over(w_site_id_by_kwh_ties).alias("kwh_rank"),
        F.round(F.avg("voltage").over(w_read_at_month_quality), 2).alias("read_at_month_quality_avg_voltage"),
        F.lead("voltage").over(w_quality_site_id_by_kwh).alias("next_voltage"),
        F.lead("voltage", 1, 0).over(w_read_at_month_site_id_by_rea).alias("next_voltage_2"),
        F.nth_value("voltage", 2).over(w_site_id_by_voltage_full).alias("second_voltage_2"),
        F.round(F.cume_dist().over(w_site_id_by_kwh_ties), 3).alias("kwh_cume_dist"),
        F.nth_value("voltage", 2).over(w_read_at_month_site_id_by_rea_2).alias("second_voltage_3"),
        F.lead("kwh", 2).over(w_quality_site_id_by_kwh_2).alias("next2_kwh"),
    )
)

annotated = (
    metrics
    .filter(F.col("kwh") < 85.7)
    .withColumn("quality_len", F.length(F.trim(F.col("quality"))))
)

sequenced = (
    annotated
    .select(
        "*",
        F.row_number().over(w_read_at_seq_quality_by_secon).alias("seq_no"),
        F.size(F.collect_set("site_id").over(w_read_at_seq)).alias("distinct_site_id_in_read_at_seq"),
        F.round(F.avg("next_voltage").over(w_read_at_seq), 2).alias("read_at_seq_avg_next_voltage"),
        F.size(F.collect_set("quality").over(w_read_at_seq)).alias("distinct_quality_in_read_at_seq"),
        F.nth_value("kwh", 2).over(w_read_at_seq_quality_by_secon_2).alias("second_kwh"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_seq), F.lit(0)), 4).alias("voltage_share_of_read_at_seq"),
    )
)

rollup_base = (
    sequenced
    .groupBy("seq_no", "read_at_seq")
    .agg(
        F.sum(F.when(F.col("quality").isin("BAD", "EST"), F.col("next_voltage"))).alias("next_voltage_quality_in_bad_est"),
        F.min("read_at").alias("first_read_at"),
        F.round(F.avg("next_voltage"), 2).alias("avg_next_voltage"),
        F.avg("quality_read_at_month_sum_kwh").alias("avg_quality_read_at_month_sum_kwh_check"),
    )
    .filter(F.col("avg_quality_read_at_month_sum_kwh_check") > 420.0)
    .drop("avg_quality_read_at_month_sum_kwh_check")
)

windowed = (
    rollup_base
    .select(
        "*",
        F.last("first_read_at").over(w_read_at_seq_by_avg_next_volt).alias("last_first_read_at"),
        F.count(F.lit(1)).over(w_first_read_at_month_read_at).alias("running_count"),
        F.round(F.cume_dist().over(w_read_at_seq_by_avg_next_volt_2), 3).alias("avg_next_voltage_cume_dist"),
        F.lag("first_read_at").over(w_first_read_at_month_read_at_2).alias("prev_first_read_at"),
    )
)

result = (
    windowed
    .select(
        "seq_no",
        "read_at_seq",
        "next_voltage_quality_in_bad_est",
        "first_read_at",
        "avg_next_voltage",
        "last_first_read_at",
        "running_count",
        "avg_next_voltage_cume_dist",
        (F.col("running_count") + F.col("read_at_seq")).alias("running_count_plus_read_at_seq"),
    )
    .orderBy(F.col("last_first_read_at").asc_nulls_last())
)
