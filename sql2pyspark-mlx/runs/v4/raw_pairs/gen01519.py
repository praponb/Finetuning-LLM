from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 3.3) & F.col("supplier_name").like("Glo%"))
    .groupBy("country")
    .agg(
        F.count("rating").alias("rating_count"),
        F.count("*").alias("cnt"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
)
