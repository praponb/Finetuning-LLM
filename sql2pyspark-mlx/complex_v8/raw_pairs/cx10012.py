from pyspark.sql import functions as F

recent = (
    df_sites
    .filter((F.col("zone") == "Z2") & (F.length(F.trim(F.col("site_name"))) >= 4))
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "commissioned_on",
    )
)

site_rows = (
    recent
    .select(
        "zone",
        F.col("commissioned_on").alias("day"),
        F.lit("site").alias("record_type"),
    )
)

site_side = (
    df_sites
    .filter(F.col("commissioned_on") < F.lit("2025-05-07").cast("date"))
    .select(
        F.col("site_name").alias("zone"),
        F.col("commissioned_on").alias("day"),
        F.lit("site_2").alias("record_type"),
    )
)

stacked = site_rows.unionByName(site_side)

result = (
    stacked
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.max("day").alias("latest_day"))
    .orderBy("record_type")
)
