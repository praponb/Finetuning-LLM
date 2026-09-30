from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .select(F.col("site_id").alias("entity_id"), F.col("id").alias("entity_id_2"))
)

reading_side = df_meter_readings.select("site_id", "id")

unioned = reading_rows.union(reading_side)

result = (
    unioned
    .groupBy("entity_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id_2").alias("total_entity_id_2"),
    )
    .orderBy("entity_id")
)
