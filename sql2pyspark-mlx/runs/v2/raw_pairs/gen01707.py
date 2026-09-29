from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 3.1)
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
    )
    .filter(F.col("tokyo_count") < 62)
    .select("city", "country_values", "tokyo_count")
)
