from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .groupBy("country")
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.sum("rating").alias("total_rating"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
)
