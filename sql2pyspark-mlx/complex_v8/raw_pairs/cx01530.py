from pyspark.sql import functions as F

valid = df_sites.filter(F.col("commissioned_on") < F.lit("2025-05-10").cast("date"))

site_rows = (
    valid
    .select(
        F.lit("site").alias("origin"),
        "id",
        F.col("commissioned_on").alias("event_date"),
        F.col("tariff").alias("label"),
    )
)

site_rows_2 = (
    df_sites
    .filter(F.col("zone").isin("Z2", "Z4"))
    .select(
        F.lit("site_2").alias("origin"),
        "id",
        "commissioned_on",
        "zone",
    )
)

result = (
    site_rows
    .union(site_rows_2)
    .distinct()
)
