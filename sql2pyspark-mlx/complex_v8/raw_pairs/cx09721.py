from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month = Window.partitionBy(F.month("read_at"))
w_all_by_kwh = Window.orderBy(F.col("kwh").desc_nulls_first(), "id")
w_all_by_kwh_full = Window.orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_readings = (
    df_meter_readings
    .filter((~F.col("quality").isin("OK", "EST")) & (F.col("voltage") > 222.3))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 1.5, 2).alias("kwh_adj"),
        F.when(F.col("voltage") >= 232.0, "A").when(F.col("voltage") >= 223.3, "B").otherwise("C").alias("voltage_tier"),
    )
)

with_windows = (
    scoped_readings
    .select(
        "id",
        "read_at",
        "kwh",
        "kwh_adj",
        "voltage_tier",
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_month), 2).alias("kwh_vs_read_at_month_avg"),
        F.lead("voltage", 3).over(w_all_by_kwh).alias("next3_voltage"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_month), F.lit(0)), 4).alias("kwh_share_of_read_at_month"),
        F.lag("kwh_adj", 1, 0).over(w_all_by_kwh).alias("prev_kwh_adj"),
        F.lag("kwh", 1, 0).over(w_all_by_kwh).alias("prev_kwh"),
        F.last("kwh").over(w_all_by_kwh_full).alias("last_kwh"),
    )
)

result = (
    with_windows
    .select(
        "id",
        F.col("next3_voltage").alias("next3_voltage_v"),
        F.col("kwh_adj").alias("m_kwh_adj"),
        F.col("kwh_share_of_read_at_month").alias("kwh_share_of_read_at_month_v"),
        F.col("prev_kwh").alias("prev_kwh_v"),
    )
    .unpivot(
        ids=["id"],
        values=["next3_voltage_v", "m_kwh_adj", "kwh_share_of_read_at_month_v", "prev_kwh_v"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric")
)
