from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(~(((F.col("city") == "Chiang Mai") & (F.col("country") != "US")) | (F.col("city") != "New York")))
)

country_summary = (
    recent_suppliers
    .groupBy("country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.max("rating").alias("max_rating"),
    )
)

result = (
    country_summary
    .filter(F.col("max_rating") > 2.0)
    .select("country", "country_values")
)
