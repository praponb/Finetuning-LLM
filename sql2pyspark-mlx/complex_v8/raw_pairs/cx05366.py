from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter((F.col("kwh") < 109.0) & (F.col("quality").isin("BAD", "OK")))
    .select(
        F.col("site_id").alias("key_id"),
        F.col("read_at").alias("happened_at"),
        "quality",
    )
)

reading_side_2 = (
    df_meter_readings
    .select(
        F.col("id").alias("key_id"),
        F.col("read_at").alias("happened_at"),
        "quality",
    )
)

combined = reading_side.unionByName(reading_side_2)

result = (
    combined
    .groupBy("quality")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("key_id").alias("total_key_id"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("quality")
)
