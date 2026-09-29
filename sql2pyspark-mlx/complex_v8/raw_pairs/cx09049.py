from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_read_at_quarter = Window.partitionBy("site_id", "read_at_quarter")
w_read_at_quarter_by_read_at_t = Window.partitionBy("read_at_quarter").orderBy(F.col("read_at").desc())
w_read_at_quarter = Window.partitionBy("read_at_quarter")
w_read_at_quarter_by_read_at_m = Window.partitionBy("read_at_quarter").orderBy("read_at", "id").rowsBetween(-6, Window.currentRow)
w_all_by_read_at_prior_2 = Window.orderBy("read_at", "id").rowsBetween(-2, -1)
w_read_at_quarter_by_read_at_r = Window.partitionBy("read_at_quarter").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_to_date_read_at_quar = Window.partitionBy(F.to_date("read_at"), "read_at_quarter").orderBy("read_at", "id")
w_read_at_to_date_read_at_quar_2 = Window.partitionBy(F.to_date("read_at"), "read_at_quarter").orderBy("read_at", "id").rowsBetween(-6, Window.currentRow)
w_read_at_quarter_by_read_at = Window.partitionBy("read_at_quarter").orderBy(F.col("read_at").desc(), "id")
w_all_by_read_at_ties = Window.orderBy("read_at")
w_read_at_quarter_by_kwh_chang = Window.partitionBy("read_at_quarter").orderBy(F.col("kwh_change").desc(), "id")
w_read_at_quarter_by_kwh_chang_2 = Window.partitionBy("read_at_quarter").orderBy(F.col("kwh_change").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .filter(F.col("site_id") >= 9)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
    )
)

windowed = (
    scoped_readings
    .select(
        "id",
        "voltage",
        "read_at_quarter",
        F.min("read_at_quarter").over(w_site_id_read_at_quarter).alias("site_id_read_at_quarter_min_read_at_quarter"),
        F.round(F.cume_dist().over(w_read_at_quarter_by_read_at_t), 3).alias("read_at_cume_dist"),
        F.max("read_at_quarter").over(w_read_at_quarter).alias("read_at_quarter_max_read_at_quarter"),
        F.max("voltage").over(w_read_at_quarter_by_read_at_m).alias("voltage_max_7"),
        F.max("voltage").over(w_read_at_quarter).alias("read_at_quarter_max_voltage"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_quarter), F.lit(0)), 4).alias("kwh_share_of_read_at_quarter"),
        F.sum("voltage").over(w_all_by_read_at_prior_2).alias("voltage_sum_2_prior"),
        F.count(F.lit(1)).over(w_read_at_quarter).alias("rows_in_read_at_quarter"),
        F.count(F.lit(1)).over(w_read_at_quarter_by_read_at_r).alias("running_count"),
        F.lag("kwh").over(w_read_at_to_date_read_at_quar).alias("prev_kwh"),
        F.round(F.avg("kwh").over(w_read_at_to_date_read_at_quar_2), 2).alias("kwh_avg_7"),
        (F.col("kwh") - F.lag("kwh").over(w_read_at_quarter_by_read_at)).alias("kwh_change"),
        F.round(F.percent_rank().over(w_all_by_read_at_ties), 3).alias("read_at_percent_rank"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.lead("read_at_percent_rank").over(w_read_at_quarter_by_kwh_chang).alias("next_read_at_percent_rank"),
        F.sum("prev_kwh").over(w_read_at_quarter_by_kwh_chang_2).alias("running_prev_kwh"),
        F.max("kwh_share_of_read_at_quarter").over(w_read_at_quarter).alias("read_at_quarter_max_kwh_share_of_read_at_quarter"),
    )
)

rollup_base = (
    with_trends
    .groupBy(
        "read_at_quarter",
        F.when(F.col("kwh_avg_7") >= 84.0, "A").when(F.col("kwh_avg_7") >= 24.0, "B").otherwise("C").alias("kwh_avg_7_tier"),
        F.when(F.col("voltage") >= 237.5, "large").when(F.col("voltage") >= 222.3, "medium").otherwise("small").alias("voltage_tier"),
    )
    .agg(
        F.round(F.sum("site_id_read_at_quarter_min_read_at_quarter") / F.nullif(F.sum("prev_kwh"), F.lit(0)), 3).alias("site_id_read_at_quarter_min_read_at_quarter_to_p"),
        F.round(F.stddev("site_id_read_at_quarter_min_read_at_quarter"), 2).alias("stddev_site_id_read_at_quarter_min_read_at_quart"),
    )
)

result = (
    rollup_base
    .select(
        "read_at_quarter",
        "kwh_avg_7_tier",
        "site_id_read_at_quarter_min_read_at_quarter_to_p",
        "stddev_site_id_read_at_quarter_min_read_at_quart",
    )
    .orderBy(F.col("stddev_site_id_read_at_quarter_min_read_at_quart").desc())
)
