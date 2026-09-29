from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") != 4590) & (F.col("city").isin(["Berlin", "Bangkok", "New York"]) | F.col("store_name").like("%Mal%")))
    .groupBy("city", "region")
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 5)
    .drop("cnt")
    .orderBy("region")
)
