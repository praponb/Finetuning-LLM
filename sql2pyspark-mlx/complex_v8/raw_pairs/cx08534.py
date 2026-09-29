from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_group_quality_by_kwh = Window.partitionBy("quality_group", "quality").orderBy(F.col("kwh").desc(), "id")
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_group_quality_by_kwh_2 = Window.partitionBy("quality_group", "quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_readings = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

annotated = (
    base_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

sequenced = (
    annotated
    .select(
        "*",
        F.lag("kwh").over(w_quality_group_quality_by_kwh).alias("prev_kwh"),
        F.sum("kwh").over(w_all_by_kwh_running).alias("running_kwh"),
        F.min("kwh").over(w_all_by_kwh_running).alias("min_kwh_so_far"),
        F.first("kwh", ignorenulls=True).over(w_quality_group_quality_by_kwh_2).alias("first_known_kwh"),
        F.row_number().over(w_quality_group_quality_by_kwh).alias("kwh_seq"),
    )
)

result = (
    sequenced
    .filter(F.col("kwh_seq") >= 3)
    .select(
        "id",
        "kwh",
        "quality",
        "running_kwh",
        "min_kwh_so_far",
        "first_known_kwh",
        "kwh_seq",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
    .orderBy("running_kwh", F.col("kwh").desc())
)
