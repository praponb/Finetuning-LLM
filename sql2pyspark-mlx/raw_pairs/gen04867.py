from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("temp_c_tier").orderBy(F.col("corr_temp_c_humidity").desc())

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.round((F.col("humidity") * 100), 2).alias("humidity_adjusted"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("temp_c") >= 40.7, "LARGE").otherwise("SMALL").alias("temp_c_tier"),
        F.when(F.col("status_bits") >= 12, "PREMIUM").otherwise("STANDARD").alias("status_bits_tier"),
    )
    .agg(
        F.corr("temp_c", "humidity").alias("corr_temp_c_humidity"),
        F.min("reading_ts").alias("first_seen"),
        F.min("humidity_adjusted").alias("min_humidity_adjusted"),
    )
    .withColumn("corr_temp_c_humidity_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("status_bits_tier").desc())
)
