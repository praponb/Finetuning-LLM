from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_last(), "id")

ranked = (
    df_device_readings
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("status_bits") >= 11, "HIGH").otherwise("LOW").alias("status_bits_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list_2"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
    )
)
