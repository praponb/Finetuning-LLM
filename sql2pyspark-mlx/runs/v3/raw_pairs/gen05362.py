from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", "region")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.sum("sqft").alias("total_sqft"),
    )
    .filter(F.col("total_sqft") > 72000)
    .select("city", "region", "region_values", "total_sqft")
)
