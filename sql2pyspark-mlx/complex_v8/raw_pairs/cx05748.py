from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter((F.col("kwh") > 52.4) & (F.col("quality") != "EST"))
    .select(
        F.lit("reading").alias("record_type"),
        F.col("id").alias("entity_id"),
        "kwh",
    )
)

site_side = (
    df_sites
    .filter(F.col("commissioned_on").isNotNull())
    .select(F.lit("site").alias("record_type"), "id", "installed_kw")
)

stacked = (
    reading_rows
    .union(site_side)
    .distinct()
)

result = (
    stacked
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.sum("kwh").alias("total_kwh"))
    .orderBy("record_type")
)
