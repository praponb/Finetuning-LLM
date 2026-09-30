from pyspark.sql import functions as F

prepared_sites = df_sites.filter(~F.col("zone").isin("Z2", "Z4"))

site_side = (
    prepared_sites
    .filter(F.col("zone").isin("Z2", "Z4"))
    .select(
        F.lit("site").alias("origin"),
        F.col("tariff").alias("name"),
        F.col("site_name").alias("category"),
    )
)

site_rows = (
    df_sites
    .select(
        F.lit("site_2").alias("origin"),
        F.col("zone").alias("name"),
        F.col("site_name").alias("category"),
    )
)

site_rows_2 = (
    df_sites
    .filter((F.col("commissioned_on") < F.lit("2024-11-21").cast("date")) & (F.col("zone").isin("Z4", "Z2")))
    .select(
        F.lit("site_3").alias("origin"),
        F.col("zone").alias("name"),
        F.col("tariff").alias("category"),
    )
)

result = (
    site_side
    .unionByName(site_rows)
    .unionByName(site_rows_2)
)
