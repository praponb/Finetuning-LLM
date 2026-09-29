from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("humidity_int").desc(), "id")
w_grp = Window.partitionBy("device_code").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("device_code").orderBy("humidity", "id")
w_running = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy("running_avg_burst_id", "id")

base = (
    df_device_readings
    .filter(F.col("device_code") == "DV-04")
    .withColumn("temp_c_rounded", F.round("temp_c", 0))
    .withColumn("humidity_int", F.col("humidity").cast("int"))
)

gaps = (
    base
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "temp_c_rounded",
        "humidity_int",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

deduped = (
    sessions_device_readings
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .filter(F.col("humidity_int") != 27)
    .select(
        "*",
        F.count("burst_id").over(w_grp).alias("running_count_burst_id"),
        F.sum("burst_id").over(w_grp).alias("running_sum_burst_id"),
        F.avg("burst_id").over(w_grp).alias("running_avg_burst_id"),
        F.ntile(4).over(w_ordered).alias("humidity_bucket"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.avg("running_count_burst_id").over(w_running).alias("running_count_burst_id_avg_5"),
        F.lead("temp_c_rounded", 2).over(w7).alias("next_temp_c_rounded"),
    )
)

kept_keys = (
    scored
    .select("id", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("firmware") != "1.1.0")
        .select("id", "device_code")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "humidity_int",
        "burst_id",
        "dedup_rank",
        "running_count_burst_id",
        "running_sum_burst_id",
        F.when(F.col("device_code").rlike("^(DV-01|DV-04)$"), "GROUP_A").when(F.col("device_code").rlike("^(DV-02|DV-03)$"), "GROUP_B").otherwise("CORE").alias("device_code_zone"),
    )
    .orderBy(F.col("dedup_rank").asc_nulls_last())
)
