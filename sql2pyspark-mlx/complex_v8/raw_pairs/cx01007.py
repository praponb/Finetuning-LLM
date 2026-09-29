from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_quality = Window.partitionBy(F.date_trunc("month", "read_at"), "quality")

filtered_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.hour(F.col("read_at")).alias("read_at_hour"),
    )
)

metrics = (
    filtered_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "kwh_filled",
        "read_at_hour",
        F.round(F.avg("read_at_hour").over(w_read_at_month_quality), 2).alias("read_at_month_quality_avg_read_at_hour"),
        F.max("kwh_filled").over(w_read_at_month_quality).alias("read_at_month_quality_max_kwh_filled"),
        F.sum("voltage").over(w_read_at_month_quality).alias("read_at_month_quality_sum_voltage"),
    )
)

result = (
    metrics
    .select(
        "id",
        "kwh",
        "read_at_month_quality_avg_read_at_hour",
        "kwh_filled",
        "read_at_month_quality_max_kwh_filled",
    )
    .unpivot(
        ids=["id"],
        values=["kwh", "read_at_month_quality_avg_read_at_hour", "kwh_filled", "read_at_month_quality_max_kwh_filled"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric")
)
