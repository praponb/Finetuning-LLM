from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_greatest_kwh_voltage = Window.orderBy("greatest_kwh_voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_greatest_kwh_voltage_2 = Window.orderBy("greatest_kwh_voltage", "id")
w_quality = Window.partitionBy("quality")

valid_readings = (
    df_meter_readings
    .filter(F.col("kwh") < 110.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage_2"),
    )
)

tagged = (
    valid_readings
    .filter(F.col("greatest_kwh_voltage") < 196.0)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "greatest_kwh_voltage",
        "kwh_bucket",
        "greatest_kwh_voltage_2",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

with_windows = (
    tagged
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "kwh_bucket",
        "quality_slug",
        "quality_or_unknown",
        F.sum("greatest_kwh_voltage").over(w_all_by_greatest_kwh_voltage).alias("running_greatest_kwh_voltage"),
        F.row_number().over(w_all_by_greatest_kwh_voltage_2).alias("seq_no"),
        (F.col("kwh_bucket") - F.lag("kwh_bucket").over(w_all_by_greatest_kwh_voltage_2)).alias("kwh_bucket_change"),
        F.round(F.avg("kwh").over(w_quality), 2).alias("quality_avg_kwh"),
        F.ntile(4).over(w_all_by_greatest_kwh_voltage_2).alias("greatest_kwh_voltage_quartile"),
        F.first("kwh_bucket", ignorenulls=True).over(w_all_by_greatest_kwh_voltage).alias("first_known_kwh_bucket"),
    )
)

result = (
    with_windows
    .select(
        "id",
        F.col("quality_avg_kwh").alias("quality_avg_kwh_v"),
        F.col("voltage").alias("m_voltage"),
        F.col("kwh").alias("m_kwh"),
    )
    .unpivot(
        ids=["id"],
        values=["quality_avg_kwh_v", "m_voltage", "m_kwh"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
