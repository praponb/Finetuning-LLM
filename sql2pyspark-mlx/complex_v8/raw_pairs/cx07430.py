from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_voltage_plus_kwh = Window.partitionBy("site_id").orderBy(F.col("voltage_plus_kwh").desc(), "id")
w_site_id_by_voltage_plus_kwh_2 = Window.partitionBy("site_id").orderBy(F.col("voltage_plus_kwh").desc(), "id").rowsBetween(-1, -1)
w_site_id_read_at_to_date = Window.partitionBy("site_id", F.to_date("read_at"))
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_read_at_month_site_id_by_vol = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id").orderBy("voltage_plus_kwh", "id")
w_site_id_by_voltage_plus_kwh_3 = Window.partitionBy("site_id").orderBy(F.col("voltage_plus_kwh").desc(), "id").rowsBetween(-2, -1)
w_site_id_by_voltage_plus_kwh_4 = Window.partitionBy("site_id").orderBy(F.col("voltage_plus_kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_moving_6 = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(-6, Window.currentRow)
w_seq_no_by_site_id_read_at_to = Window.partitionBy("seq_no").orderBy("site_id_read_at_to_date_min_voltage_plus_kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_read_at_month_seq_no = Window.partitionBy(F.date_trunc("month", "second_read_at"), "seq_no")
w_seq_no_by_seq_no_moving_1 = Window.partitionBy("seq_no").orderBy("seq_no", "id").rowsBetween(-1, Window.currentRow)

filtered_readings = (
    df_meter_readings
    .filter((F.col("kwh") >= 20.4) & (F.col("quality").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
    )
)

sequenced = (
    filtered_readings
    .select(
        "id",
        "site_id",
        F.row_number().over(w_site_id_by_voltage_plus_kwh).alias("seq_no"),
        F.sum("voltage_plus_kwh").over(w_site_id_by_voltage_plus_kwh_2).alias("voltage_plus_kwh_sum_1_prior"),
        F.count(F.lit(1)).over(w_site_id_read_at_to_date).alias("rows_in_site_id_read_at_to_date"),
        (F.col("voltage_plus_kwh") - F.lag("voltage_plus_kwh").over(w_all_by_kwh)).alias("voltage_plus_kwh_change"),
        F.sum("kwh").over(w_site_id_read_at_to_date).alias("site_id_read_at_to_date_sum_kwh"),
        F.lead("read_at", 2).over(w_read_at_month_site_id_by_vol).alias("next2_read_at"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id_read_at_to_date), 2).alias("voltage_vs_site_id_read_at_to_date_avg"),
        F.round(F.avg("kwh").over(w_site_id_by_voltage_plus_kwh_3), 2).alias("kwh_avg_2_prior"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id_read_at_to_date), F.lit(0)), 4).alias("voltage_share_of_site_id_read_at_to_date"),
        F.min("voltage_plus_kwh").over(w_site_id_read_at_to_date).alias("site_id_read_at_to_date_min_voltage_plus_kwh"),
        F.nth_value("read_at", 2).over(w_site_id_by_voltage_plus_kwh_4).alias("second_read_at"),
        F.sum("kwh").over(w_all_by_kwh_moving_6).alias("kwh_sum_7"),
    )
)

top_per_group = sequenced.filter((F.col("seq_no") == 1) & (F.col("kwh_avg_2_prior") > 42.0))

ranked = (
    top_per_group
    .select(
        "id",
        "site_id",
        "rows_in_site_id_read_at_to_date",
        "site_id_read_at_to_date_sum_kwh",
        "next2_read_at",
        "voltage_vs_site_id_read_at_to_date_avg",
        "voltage_share_of_site_id_read_at_to_date",
        "kwh_sum_7",
        F.last("seq_no", ignorenulls=True).over(w_seq_no_by_site_id_read_at_to).alias("last_known_seq_no"),
        F.round(F.col("kwh_avg_2_prior") - F.avg("kwh_avg_2_prior").over(w_second_read_at_month_seq_no), 2).alias("kwh_avg_2_prior_vs_second_read_at_month_seq_no_a"),
        F.size(F.collect_set("site_id").over(w_second_read_at_month_seq_no)).alias("distinct_site_id_in_second_read_at_month_seq_no"),
        F.sum("voltage_plus_kwh_sum_1_prior").over(w_second_read_at_month_seq_no).alias("second_read_at_month_seq_no_sum_voltage_plus_kwh"),
        F.round(F.avg("voltage_plus_kwh_change").over(w_seq_no_by_seq_no_moving_1), 2).alias("voltage_plus_kwh_change_avg_2"),
    )
)

result = (
    ranked
    .filter(F.col("voltage_share_of_site_id_read_at_to_date") > 0.2)
    .select(
        "id",
        "site_id",
        "next2_read_at",
        "voltage_vs_site_id_read_at_to_date_avg",
        "voltage_share_of_site_id_read_at_to_date",
        "kwh_sum_7",
        "last_known_seq_no",
        "kwh_avg_2_prior_vs_second_read_at_month_seq_no_a",
        "distinct_site_id_in_second_read_at_month_seq_no",
        "second_read_at_month_seq_no_sum_voltage_plus_kwh",
        "voltage_plus_kwh_change_avg_2",
        F.abs(F.col("kwh_sum_7") - 198.0).alias("kwh_sum_7_distance"),
    )
)
