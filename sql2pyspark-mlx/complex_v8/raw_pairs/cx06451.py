from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter(F.col("zone") == "Z3")
    .select("tariff", F.col("id").alias("ref_id"))
)

site_side_2 = df_sites.select("site_name", "id")

all_events = site_side.union(site_side_2)

result = (
    all_events
    .groupBy("tariff")
    .agg(F.count("*").alias("n_rows"), F.sum("ref_id").alias("total_ref_id"))
    .orderBy("tariff")
)
