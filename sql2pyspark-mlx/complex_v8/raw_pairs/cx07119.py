from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("greatest_voltage_kwh").desc(), F.col("id").desc())
w_site_id_voltage_tier_by_kwh = Window.partitionBy("site_id", "voltage_tier").orderBy("kwh")
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")
w_site_id = Window.partitionBy("site_id")
w_site_id_voltage_tier_by_kwh_2 = Window.partitionBy("site_id", "voltage_tier").orderBy("kwh", "id").rowsBetween(-6, Window.currentRow)
w_all_by_voltage_moving_6 = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_voltage_ties = Window.orderBy(F.col("voltage").desc())

base_readings = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp")) | (F.col("site_id") <= 7))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.greatest(F.col("voltage"), F.col("kwh")).alias("greatest_voltage_kwh"),
        F.when(F.col("voltage") >= 232.0, "HIGH").when(F.col("voltage") >= 222.5, "MEDIUM").otherwise("LOW").alias("voltage_tier"),
    )
)

latest_per_key = (
    base_readings
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    latest_per_key
    .select(
        "*",
        F.round(F.cume_dist().over(w_site_id_voltage_tier_by_kwh), 3).alias("kwh_cume_dist"),
        F.row_number().over(w_all_by_voltage).alias("rn"),
        F.sum("greatest_voltage_kwh").over(w_site_id).alias("site_id_sum_greatest_voltage_kwh"),
        F.round(F.avg("kwh").over(w_site_id_voltage_tier_by_kwh_2), 2).alias("kwh_avg_7"),
        F.round(F.avg("voltage").over(w_all_by_voltage_moving_6), 2).alias("voltage_avg_7"),
        F.dense_rank().over(w_all_by_voltage_ties).alias("voltage_dense_rank"),
    )
)

derived = (
    metrics
    .withColumn("voltage_tier_group", F.when(F.col("voltage_tier").isin("HIGH", "LOW"), "group_a").when(F.col("voltage_tier") == "MEDIUM", "group_b").otherwise("other"))
)

result = (
    derived
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "greatest_voltage_kwh",
        "voltage_tier",
        "rn",
        "kwh_avg_7",
        "voltage_avg_7",
        "voltage_dense_rank",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
    .orderBy(F.col("quality_slug").desc(), F.col("voltage_dense_rank").desc())
)
