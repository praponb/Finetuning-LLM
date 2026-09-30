from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date = Window.partitionBy(F.to_date("read_at"))
w_read_at_month_site_id_by_rea = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month_site_id_by_rea_2 = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id").orderBy("read_at", "id")
w_read_at_month_read_at_dense = Window.partitionBy(F.month("read_at"), "read_at_dense_rank")
w_all_by_read_at = Window.orderBy("read_at", "id")
w_all_by_read_at_running = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_readings = (
    df_meter_readings
    .filter(F.col("kwh") <= 81.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

screened = (
    base_readings
    .crossJoin(base_readings.agg(F.max("kwh").alias("overall_max_kwh")))
    .filter(F.col("kwh") < F.col("overall_max_kwh"))
    .drop("overall_max_kwh")
)

windowed = (
    screened
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        F.max("kwh").over(w_read_at_to_date).alias("read_at_to_date_max_kwh"),
        F.first("kwh", ignorenulls=True).over(w_read_at_month_site_id_by_rea).alias("first_known_kwh"),
        F.dense_rank().over(w_read_at_month_site_id_by_rea_2).alias("read_at_dense_rank"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_read_at_month_site_id_by_rea_2))) / 60, 1).alias("minutes_since_prev_read_at"),
    )
)

ranked = (
    windowed
    .select(
        "*",
        F.round(F.col("minutes_since_prev_read_at") - F.avg("minutes_since_prev_read_at").over(w_read_at_month_read_at_dense), 2).alias("minutes_since_prev_read_at_vs_read_at_month_read"),
        F.lag("read_at_dense_rank", 2).over(w_all_by_read_at).alias("prev2_read_at_dense_rank"),
        F.last("minutes_since_prev_read_at", ignorenulls=True).over(w_all_by_read_at_running).alias("last_known_minutes_since_prev_read_at"),
    )
)

result = (
    ranked
    .select(
        "id",
        "read_at",
        "kwh",
        "read_at_to_date_max_kwh",
        "first_known_kwh",
        "minutes_since_prev_read_at_vs_read_at_month_read",
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
        F.abs(F.col("prev2_read_at_dense_rank") - 6).alias("prev2_read_at_dense_rank_distance"),
    )
    .orderBy(F.col("read_at_to_date_max_kwh").desc_nulls_first(), "id")
)
