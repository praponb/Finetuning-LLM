from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_read_at_full = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_read_at_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month_site_id = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_read_at_month = Window.partitionBy(F.month("read_at"))
w_read_at_month_by_voltage_cha = Window.partitionBy(F.month("read_at")).orderBy("voltage_change", "id").rowsBetween(-6, -1)
w_read_at_month_by_voltage_cha_2 = Window.partitionBy(F.month("read_at")).orderBy("voltage_change", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage_change_prior = Window.orderBy(F.col("voltage_change").desc(), "id").rowsBetween(-3, -1)
w_read_at_month_by_voltage_cha_3 = Window.partitionBy(F.month("read_at")).orderBy("voltage_change")

filtered = (
    df_meter_readings
    .filter((F.col("kwh") < 86.0) & (F.col("voltage") <= 239.6))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

windowed = (
    filtered
    .select(
        "id",
        "read_at",
        "voltage",
        F.first("kwh").over(w_site_id_by_read_at_full).alias("first_kwh"),
        F.last("read_at", ignorenulls=True).over(w_site_id_by_read_at_running).alias("last_known_read_at"),
        F.count(F.lit(1)).over(w_read_at_month_site_id).alias("rows_in_read_at_month_site_id"),
        (F.col("voltage") - F.lag("voltage").over(w_site_id_by_read_at)).alias("voltage_change"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.count(F.lit(1)).over(w_read_at_month).alias("rows_in_read_at_month"),
        F.max("voltage").over(w_read_at_month_by_voltage_cha).alias("voltage_max_6_prior"),
        F.count(F.lit(1)).over(w_read_at_month_by_voltage_cha_2).alias("running_count"),
        F.sum("voltage_change").over(w_read_at_month).alias("read_at_month_sum_voltage_change"),
        F.round(F.avg("first_kwh").over(w_all_by_voltage_change_prior), 2).alias("first_kwh_avg_3_prior"),
        F.dense_rank().over(w_read_at_month_by_voltage_cha_3).alias("voltage_change_dense_rank"),
    )
)

enriched = sequenced.withColumn("read_at_week", F.date_trunc("week", F.col("read_at")))

result = (
    enriched
    .select(
        "id",
        "read_at",
        "voltage",
        "first_kwh",
        "last_known_read_at",
        "rows_in_read_at_month_site_id",
        "voltage_change",
        "rows_in_read_at_month",
        "running_count",
        "read_at_month_sum_voltage_change",
        "first_kwh_avg_3_prior",
        "voltage_change_dense_rank",
        "read_at_week",
        F.date_trunc("week", F.col("last_known_read_at")).alias("last_known_read_at_week"),
        F.date_trunc("day", F.col("last_known_read_at")).alias("last_known_read_at_day"),
    )
    .orderBy("voltage", F.col("first_kwh_avg_3_prior").asc(), "id")
    .limit(20)
)
