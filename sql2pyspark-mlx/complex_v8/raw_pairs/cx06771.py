from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-12 08:00:00").cast("timestamp"))
    .select(F.col("id").alias("ref_id"))
)

site_rows = (
    df_sites
    .filter((F.col("commissioned_on") < F.lit("2025-05-10").cast("date")) & (F.col("site_name").isNotNull()))
    .select(F.col("id").alias("ref_id"))
)

result = (
    reading_side
    .subtract(site_rows)
    .orderBy(F.col("ref_id").desc())
)
