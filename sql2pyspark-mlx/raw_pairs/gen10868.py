from pyspark.sql import functions as F

src = df_stores.filter(F.col("city") == "Chiang Mai")

agg = (
    src
    .groupBy(
        F.when(F.col("sqft") >= 15473, "high").when(F.col("sqft") >= 6455, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
)

result = (
    agg
    .select("sqft_bucket", "city_values")
    .orderBy("sqft_bucket")
    .limit(3)
)
