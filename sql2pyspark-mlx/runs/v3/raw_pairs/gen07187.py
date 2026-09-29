from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-10-07", "2025-08-01") & F.upper("store_name").like("%IVE%") & F.col("store_name").like("Riv%"))
    .groupBy("region")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("sqft").alias("sqft_count"),
    )
    .filter(F.col("avg_sqft") >= 52)
    .orderBy(F.col("sqft_count").desc())
)
