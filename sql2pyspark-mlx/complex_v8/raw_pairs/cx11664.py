from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_read_at_y = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality_by_read_at_y_2 = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at_year").desc(), "id")
w_quality_by_read_at = Window.partitionBy("quality").orderBy("read_at", "id")
w_site_id_by_read_at_year_prio = Window.partitionBy("site_id").orderBy("read_at_year", "id").rowsBetween(-3, -1)
w_read_at_year_month_quality = Window.partitionBy(F.date_trunc("month", "read_at_year"), "quality")

valid_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage").between(213.5, 241.9)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
)

metrics = (
    valid_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "quality_clean",
        F.last("read_at", ignorenulls=True).over(w_site_id_quality_by_read_at_y).alias("last_known_read_at"),
        F.rank().over(w_site_id_quality_by_read_at_y_2).alias("read_at_year_rank"),
        F.ntile(4).over(w_quality_by_read_at).alias("read_at_quartile"),
        F.max("kwh").over(w_site_id_by_read_at_year_prio).alias("kwh_max_3_prior"),
        F.sum("voltage").over(w_read_at_year_month_quality).alias("read_at_year_month_quality_sum_voltage"),
    )
)

with_activity = (
    metrics
    .crossJoin(metrics.agg(F.avg("read_at_year_month_quality_sum_voltage").alias("overall_avg_read_at_year_month_quality_sum_volta")))
    .filter(F.col("read_at_year_month_quality_sum_voltage") > F.col("overall_avg_read_at_year_month_quality_sum_volta"))
    .drop("overall_avg_read_at_year_month_quality_sum_volta")
)

result = (
    with_activity
    .select("id", "read_at_year_month_quality_sum_voltage", "kwh_max_3_prior")
    .unpivot(
        ids=["id"],
        values=["read_at_year_month_quality_sum_voltage", "kwh_max_3_prior"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "measure")
)
