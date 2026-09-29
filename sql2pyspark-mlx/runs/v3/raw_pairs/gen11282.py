from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.count("*").alias("cnt"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
    .orderBy(F.col("rating_range").desc())
)
