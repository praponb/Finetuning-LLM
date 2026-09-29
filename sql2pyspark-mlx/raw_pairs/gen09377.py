from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 12514, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.count("*").alias("cnt"),
    )
)
