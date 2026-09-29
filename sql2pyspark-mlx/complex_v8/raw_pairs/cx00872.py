from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .select(
        F.col("id").alias("entity_id"),
        "voltage",
        "quality",
        F.lit("reading").alias("source"),
    )
)

site_rows = (
    df_sites
    .filter((F.length(F.trim(F.col("site_name"))) >= 5) & (F.col("zone") == "Z2"))
    .select(
        "id",
        "installed_kw",
        "tariff",
        F.lit("site").alias("source"),
    )
)

combined = reading_side.union(site_rows)

result = (
    combined
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
    )
    .orderBy("source")
)
