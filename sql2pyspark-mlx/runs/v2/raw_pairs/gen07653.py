from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("rating").isNull())
    .withColumn("country_upper", F.upper("country"))
)

country_summary = (
    src
    .groupBy("country", "city")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
)

result = country_summary
