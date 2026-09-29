from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_site_id_by_read = Window.partitionBy(F.year("read_at"), "site_id").orderBy(F.col("read_at").desc(), "id")
w_site_id = Window.partitionBy("site_id")
w_read_at_year_site_id_by_read_2 = Window.partitionBy(F.year("read_at"), "site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_year_site_id_by_read_3 = Window.partitionBy(F.year("read_at"), "site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_year_by_kwh_change_f = Window.partitionBy(F.year("read_at")).orderBy("kwh_change", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month = Window.partitionBy(F.date_trunc("month", "read_at"))
w_read_at_year_by_kwh_change = Window.partitionBy(F.year("read_at")).orderBy("kwh_change", "id")
w_read_at_to_date_by_kwh_full = Window.partitionBy(F.to_date("read_at")).orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_meter_readings
    .filter(F.col("kwh") < 90.6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

ranked = (
    valid
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality_ref",
        F.row_number().over(w_read_at_year_site_id_by_read).alias("rn"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
        (F.col("kwh") - F.lag("kwh").over(w_read_at_year_site_id_by_read)).alias("kwh_change"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.sum("voltage").over(w_read_at_year_site_id_by_read_2).alias("running_voltage"),
        F.nth_value("voltage", 2).over(w_read_at_year_site_id_by_read_3).alias("second_voltage"),
    )
)

top_per_group = (
    ranked
    .filter(F.col("rn") <= 3)
    .drop("rn")
)

sequenced = (
    top_per_group
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality_ref",
        "voltage_share_of_site_id",
        "kwh_change",
        "kwh_vs_site_id_avg",
        "running_voltage",
        "second_voltage",
        F.nth_value("second_voltage", 2).over(w_read_at_year_by_kwh_change_f).alias("second_second_voltage"),
        F.round(F.col("kwh_change") / F.nullif(F.sum("kwh_change").over(w_read_at_month), F.lit(0)), 4).alias("kwh_change_share_of_read_at_month"),
        F.rank().over(w_read_at_year_by_kwh_change).alias("kwh_change_rank"),
        F.nth_value("running_voltage", 2).over(w_read_at_to_date_by_kwh_full).alias("second_running_voltage"),
    )
)

result = (
    sequenced
    .select(
        "id",
        F.col("kwh_vs_site_id_avg").alias("m_kwh_vs_site_id_avg"),
        F.col("second_running_voltage").alias("second_running_voltage_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_kwh_vs_site_id_avg", "second_running_voltage_v"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
