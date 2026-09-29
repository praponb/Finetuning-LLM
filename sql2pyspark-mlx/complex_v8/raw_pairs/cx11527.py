from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_site_id_read_at_month_by_kwh = Window.partitionBy("site_id", F.month("read_at")).orderBy("kwh", "id")
w_all_by_read_at_full = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

with_windows = (
    prepared_readings
    .select(
        "*",
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality), F.lit(0)), 4).alias("kwh_share_of_quality"),
        (F.col("kwh") - F.lag("kwh").over(w_site_id_read_at_month_by_kwh)).alias("kwh_change"),
        F.first("read_at").over(w_all_by_read_at_full).alias("first_read_at"),
        F.max("kwh").over(w_quality).alias("quality_max_kwh"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "kwh",
        "quality_max_kwh",
        "kwh_share_of_quality",
    )
    .unpivot(
        ids=["id"],
        values=["kwh", "quality_max_kwh", "kwh_share_of_quality"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
