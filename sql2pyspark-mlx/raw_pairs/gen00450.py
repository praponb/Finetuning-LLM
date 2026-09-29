from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("status_bits", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("first_known_pressure", "id").rowsBetween(-3, -1)
w5 = Window.orderBy("temp_c_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(F.col("temp_c") > 21)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.col("temp_c").cast("int").alias("temp_c_int"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "humidity",
        "pressure",
        "status_bits",
        "temp_c_int",
        F.max("humidity").over(w).alias("running_max_humidity"),
        F.sum("humidity").over(w).alias("running_sum_humidity"),
        F.count("humidity").over(w).alias("running_count_humidity"),
        F.first("pressure", ignorenulls=True).over(w2).alias("first_known_pressure"),
    )
)

scored = (
    enriched
    .withColumn("last3_avg_first_known_pressure", F.avg("first_known_pressure").over(w_running))
    .withColumn("last3_count_first_known_pressure", F.count("first_known_pressure").over(w_running))
    .withColumn("last3_max_first_known_pressure", F.max("first_known_pressure").over(w_running))
)

metrics = (
    scored
    .select(
        "id",
        "humidity",
        "running_sum_humidity",
        "first_known_pressure",
        "last3_count_first_known_pressure",
        "last3_max_first_known_pressure",
        F.sum("pressure").over(w2).alias("running_pressure"),
        F.avg("running_max_humidity").over(w4).alias("running_max_humidity_avg_3_prior"),
        F.sum("running_max_humidity").over(w5).alias("running_running_max_humidity"),
    )
)

result = (
    metrics
    .withColumn("running_sum_humidity_tier", F.when(F.col("running_sum_humidity") >= 330, "LARGE").otherwise("SMALL"))
    .withColumn("last3_max_first_known_pressure_tier", F.when(F.col("last3_max_first_known_pressure") >= 1028, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["running_sum_humidity_tier", "last3_max_first_known_pressure_tier"], ["running_sum_humidity_tier"], []],
        "running_sum_humidity_tier",
        "last3_max_first_known_pressure_tier",
    )
    .agg(
        F.round(F.avg("first_known_pressure"), 2).alias("avg_first_known_pressure"),
        F.percentile_approx("running_pressure", [0.25, 0.5, 0.75]).alias("running_pressure_quartiles"),
        F.percentile_approx("running_running_max_humidity", [0.25, 0.5, 0.75]).alias("running_running_max_humidity_quartiles"),
        F.grouping("running_sum_humidity_tier").alias("grouping_id"),
    )
    .orderBy(F.col("running_sum_humidity_tier").desc())
)
