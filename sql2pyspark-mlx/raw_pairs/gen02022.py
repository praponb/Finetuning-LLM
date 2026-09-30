from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") < 4615)
    .groupBy(
        F.when(F.col("sqft") >= 12750, "high").when(F.col("sqft") >= 2586, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
)
