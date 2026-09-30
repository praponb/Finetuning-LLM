from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("humidity", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last())

enriched = (
    df_device_readings
    .select(
        "id",
        "pressure",
        F.lead("status_bits").over(w).alias("next_status_bits"),
        F.lead("pressure").over(w).alias("next_pressure"),
        F.lag("pressure").over(w).alias("prev_pressure"),
        F.row_number().over(w).alias("event_seq"),
        F.rank().over(w2).alias("humidity_rank"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("humidity_rank") >= 7, "HIGH").otherwise("LOW").alias("humidity_rank_tier"),
        F.when(F.col("next_pressure") >= 1017, "HIGH").otherwise("LOW").alias("next_pressure_tier"),
    )
    .agg(
        F.min("event_seq").alias("min_event_seq"),
        F.round(F.avg("next_status_bits"), 2).alias("avg_next_status_bits"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("next_pressure_tier").desc_nulls_first())
)
