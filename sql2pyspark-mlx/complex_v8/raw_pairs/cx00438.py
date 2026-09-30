from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.col("tariff").isin("FLAT", "EV")) & (F.col("zone") == "Z4"))
    .select(
        F.lit("site").alias("src"),
        F.col("id").alias("entity_id"),
        "commissioned_on",
    )
)

site_rows = (
    df_sites
    .filter(F.col("commissioned_on") < F.lit("2025-05-07").cast("date"))
    .select(F.lit("site_2").alias("src"), "id", "commissioned_on")
)

site_side_2 = df_sites.select(F.lit("site_3").alias("src"), "id", "commissioned_on")

all_events = (
    site_side
    .union(site_rows)
    .union(site_side_2)
)

result = (
    all_events
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
        F.max("commissioned_on").alias("latest_commissioned_on"),
    )
    .orderBy("src")
)
