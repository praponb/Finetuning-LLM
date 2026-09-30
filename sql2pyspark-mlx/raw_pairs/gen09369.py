from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").asc_nulls_first(), "id")

ranked = (
    df_device_readings
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("temp_c") >= 34.9, "HIGH").otherwise("LOW").alias("temp_c_tier"),
    )
    .agg(
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("humidity")).otherwise(0)).alias("dv_01_humidity"),
    )
    .filter(F.col("dv_01_humidity") >= 0)
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
