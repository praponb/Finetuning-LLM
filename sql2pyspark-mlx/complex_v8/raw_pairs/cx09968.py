from pyspark.sql import functions as F

site_side = (
    df_sites
    .select(
        F.col("id").alias("key_id"),
        "commissioned_on",
        "installed_kw",
        F.lit("site").alias("origin"),
    )
)

site_rows = (
    df_sites
    .select(
        F.col("id").alias("key_id"),
        "commissioned_on",
        "installed_kw",
        F.lit("site_2").alias("origin"),
    )
)

site_rows_2 = (
    df_sites
    .select(
        F.col("id").alias("key_id"),
        "commissioned_on",
        "installed_kw",
        F.lit("site_3").alias("origin"),
    )
)

unioned = (
    site_side
    .unionByName(site_rows)
    .unionByName(site_rows_2)
)

result = (
    unioned
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("installed_kw").alias("total_installed_kw"),
        F.max("commissioned_on").alias("latest_commissioned_on"),
    )
    .orderBy("origin")
)
