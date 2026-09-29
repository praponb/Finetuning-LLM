from pyspark.sql import functions as F

site_side = (
    df_sites
    .select(
        F.lit("site").alias("src"),
        "zone",
        F.col("installed_kw").alias("metric"),
        F.col("id").alias("ref_id"),
    )
)

reading_rows = (
    df_meter_readings
    .filter((F.col("voltage").between(215.1, 239.6)) & (F.col("site_id").isNotNull()))
    .select(
        F.lit("reading").alias("src"),
        "quality",
        "kwh",
        "id",
    )
)

all_events = site_side.union(reading_rows)

result = (
    all_events
    .groupBy("src")
    .agg(F.count("*").alias("n_rows"), F.sum("metric").alias("total_metric"))
    .orderBy("src")
)
