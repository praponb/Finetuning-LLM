from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w_running = Window.partitionBy("device_code").orderBy("status_bits", "id")
w4 = Window.partitionBy("device_code").orderBy("next_humidity_plus_pressure", "id").rowsBetween(-6, -1)
w_grp = Window.partitionBy("device_code")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .select(
        "*",
        (F.col("humidity") + F.col("pressure")).alias("humidity_plus_pressure"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "device_code", "status_bits", "firmware", "humidity_plus_pressure")
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .filter(F.col("rn") < 1)
    .select(
        "id",
        "device_code",
        "status_bits",
        "humidity_plus_pressure",
        F.lag("status_bits").over(w_running).alias("prev_status_bits"),
        F.lag("humidity_plus_pressure").over(w_running).alias("prev_humidity_plus_pressure"),
        F.lead("humidity_plus_pressure").over(w_running).alias("next_humidity_plus_pressure"),
        F.row_number().over(w_running).alias("event_seq"),
        F.ntile(5).over(w2).alias("status_bits_bucket"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "status_bits",
        "prev_status_bits",
        "next_humidity_plus_pressure",
        F.avg("prev_status_bits").over(w4).alias("prev_status_bits_avg_6_prior"),
        F.count("humidity_plus_pressure").over(w_grp).alias("device_code_count_humidity_plus_pressure"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("status_bits") >= 10, "PREMIUM").otherwise("STANDARD").alias("status_bits_tier"),
    )
    .agg(
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.round(F.avg("device_code_count_humidity_plus_pressure"), 2).alias("avg_device_code_count_humidity_plus_pressure"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.percentile_approx("next_humidity_plus_pressure", [0.25, 0.5, 0.75]).alias("next_humidity_plus_pressure_quartiles"),
        F.bit_and("status_bits").alias("status_bits_bit_and_2"),
    )
)
