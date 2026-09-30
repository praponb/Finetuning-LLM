from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%uro%") & (F.col("country") != "DE"))
    .groupBy("country", "city")
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .orderBy(F.col("country").desc())
)
