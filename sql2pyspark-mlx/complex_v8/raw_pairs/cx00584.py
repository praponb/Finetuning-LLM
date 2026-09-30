from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_kwh_tier_by_read_at_ties = Window.partitionBy("kwh_tier").orderBy(F.col("read_at").desc())
w_kwh_tier_by_read_at = Window.partitionBy("kwh_tier").orderBy(F.col("read_at").desc(), "id")

valid_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.month(F.col("read_at")).isin(1, 2)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("voltage") - 220.2).alias("voltage_distance"),
    )
)

derived = (
    valid_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "voltage_distance",
        F.when(F.col("kwh") >= 78.6, "large").when(F.col("kwh") >= 42.1, "medium").otherwise("small").alias("kwh_tier"),
    )
)

windowed = (
    derived
    .select(
        "id",
        "read_at",
        "quality",
        "voltage_distance",
        "kwh_tier",
        F.round(F.percent_rank().over(w_kwh_tier_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.row_number().over(w_kwh_tier_by_read_at).alias("seq_no"),
        F.round(F.cume_dist().over(w_kwh_tier_by_read_at), 3).alias("read_at_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "id",
        "read_at",
        "quality",
        "voltage_distance",
        "kwh_tier",
        "read_at_percent_rank",
        "read_at_cume_dist",
    )
    .orderBy("read_at_cume_dist")
)
