from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_quality_by_read = Window.partitionBy(F.year("read_at"), "quality").orderBy(F.col("read_at").desc(), "id").rowsBetween(-2, -1)
w_all_by_voltage_running = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_ties = Window.orderBy(F.col("kwh").desc())
w_read_at_year_quality_by_read_2 = Window.partitionBy(F.year("read_at"), "quality").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_to_date = Window.partitionBy(F.to_date("read_at"))
w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_all_by_read_at_prior_3 = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(-3, -1)

filtered = (
    df_meter_readings
    .filter(F.col("voltage") >= 224.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

ranked = (
    filtered
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        F.max("kwh").over(w_read_at_year_quality_by_read).alias("kwh_max_2_prior"),
        F.sum("kwh").over(w_all_by_voltage_running).alias("running_kwh"),
        F.dense_rank().over(w_all_by_kwh_ties).alias("kwh_dense_rank"),
        F.max("kwh").over(w_read_at_year_quality_by_read_2).alias("max_kwh_so_far"),
        F.max("kwh").over(w_read_at_to_date).alias("read_at_to_date_max_kwh"),
        F.nth_value("read_at", 2).over(w_site_id_by_voltage_full).alias("second_read_at"),
        F.last("kwh", ignorenulls=True).over(w_site_id_by_voltage_running).alias("last_known_kwh"),
        F.lag("kwh", 3).over(w_all_by_read_at).alias("prev3_kwh"),
        F.lead("voltage", 1, 0).over(w_all_by_kwh).alias("next_voltage"),
        F.size(F.collect_set("quality").over(w_read_at_to_date)).alias("distinct_quality_in_read_at_to_date"),
        F.lag("read_at", 2).over(w_all_by_kwh).alias("prev2_read_at"),
        F.round(F.avg("kwh").over(w_all_by_read_at_prior_3), 2).alias("kwh_avg_3_prior"),
    )
)

result = (
    ranked
    .filter(F.col("kwh_dense_rank") <= 7)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "kwh_max_2_prior",
        "kwh_dense_rank",
        "max_kwh_so_far",
        "read_at_to_date_max_kwh",
        "second_read_at",
        "last_known_kwh",
        "next_voltage",
        "distinct_quality_in_read_at_to_date",
        "prev2_read_at",
        "kwh_avg_3_prior",
    )
    .orderBy(F.col("max_kwh_so_far").asc())
)
