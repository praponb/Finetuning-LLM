from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter((F.col("quality").isNotNull()) & (F.col("voltage") >= 223.0))
    .select("site_id", "quality", F.col("read_at").alias("happened_at"))
)

tariff_side = df_tariff_windows.select("id", "tariff", "valid_to")

combined = reading_side.union(tariff_side)

result = (
    combined
    .groupBy("quality")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("site_id").alias("total_site_id"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("quality")
)
