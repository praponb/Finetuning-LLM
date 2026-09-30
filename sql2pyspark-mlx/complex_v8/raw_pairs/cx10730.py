from pyspark.sql import functions as F

reading_side = df_meter_readings.select("site_id", F.col("voltage").alias("amount"))

site_rows = df_sites.select("id", "installed_kw")

all_events = reading_side.union(site_rows)

result = (
    all_events
    .groupBy("site_id")
    .agg(F.count("*").alias("n_rows"), F.sum("amount").alias("total_amount"))
    .orderBy("site_id")
)
