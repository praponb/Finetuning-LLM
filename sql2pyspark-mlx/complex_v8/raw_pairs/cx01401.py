from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .select(
        F.lit("reading").alias("source"),
        F.col("id").alias("qty"),
        "quality",
        F.col("site_id").alias("entity_id"),
    )
)

reading_side_2 = (
    df_meter_readings
    .filter((F.col("voltage").between(213.5, 241.9)) & (F.col("kwh") <= 81.3))
    .select(
        F.lit("reading_2").alias("source"),
        F.col("id").alias("qty"),
        "quality",
        F.col("site_id").alias("entity_id"),
    )
)

combined = (
    reading_side
    .unionByName(reading_side_2)
    .distinct()
)

result = (
    combined
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
    )
    .orderBy("source")
)
