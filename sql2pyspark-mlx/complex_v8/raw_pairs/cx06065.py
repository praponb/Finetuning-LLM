from pyspark.sql import functions as F

site_side = (
    df_sites
    .select(
        F.lit("site").alias("source"),
        "id",
        "commissioned_on",
        F.col("installed_kw").alias("metric"),
    )
)

site_side_2 = (
    df_sites
    .filter((F.length(F.trim(F.col("site_name"))) >= 5) & (F.col("zone").isin("Z2", "Z4")))
    .select(
        F.lit("site_2").alias("source"),
        "id",
        "commissioned_on",
        "installed_kw",
    )
)

result = (
    site_side
    .union(site_side_2)
    .distinct()
    .orderBy(F.col("source").desc(), "metric")
)
