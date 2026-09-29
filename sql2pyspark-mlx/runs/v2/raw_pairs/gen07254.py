from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.col("city") == "Chiang Mai") & (F.upper(F.trim("store_name")) == "DOWNTOWN EXPRESS")) | (F.col("city") == "Bangkok"))
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
    .orderBy("city")
)
