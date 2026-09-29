from pyspark.sql import functions as F

site_side = df_sites.select(F.lit("site").alias("record_type"), "id", "commissioned_on")

site_side_2 = df_sites.select(F.lit("site_2").alias("record_type"), "id", "commissioned_on")

combined = site_side.union(site_side_2)

result = (
    combined
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("commissioned_on").alias("latest_commissioned_on"),
    )
    .orderBy("record_type")
)
