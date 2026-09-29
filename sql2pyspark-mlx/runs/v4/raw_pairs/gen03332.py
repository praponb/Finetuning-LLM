from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country", "city")
    .agg(
        F.count("*").alias("total_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .orderBy("country")
)
