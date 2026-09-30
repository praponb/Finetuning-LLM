from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("duration_sec_band", F.when(F.col("duration_sec") >= 439, "High").when(F.col("duration_sec") >= 86, "Medium").otherwise("Low"))
    .withColumn("duration_sec_whole", F.floor("duration_sec"))
    .orderBy("page")
)
