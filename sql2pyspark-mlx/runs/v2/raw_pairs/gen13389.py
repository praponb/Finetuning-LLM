from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "Bangkok", "Berlin"]))
    .withColumn("city_country_key", F.concat_ws("-", "city", "country"))
)

city_summary = (
    src
    .groupBy("city")
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)

result = (
    city_summary
    .select("city", "country_list")
    .orderBy(F.col("city").desc())
)
