from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter(F.month(F.col("valid_to")).isin(2, 3))
    .select("id", F.col("rate").alias("value"))
)

site_side = df_sites.select("id", F.col("installed_kw").alias("value"))

combined = tariff_side.unionByName(site_side)

result = (
    combined
    .groupBy("id")
    .agg(F.count("*").alias("n_rows"), F.sum("value").alias("total_value"))
    .orderBy("id")
)
