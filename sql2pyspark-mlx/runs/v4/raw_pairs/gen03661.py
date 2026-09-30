from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 13013, "high").when(F.col("sqft") >= 6316, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
    .orderBy("sqft_bucket")
)
