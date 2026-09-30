from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.count("*").alias("num_suppliers"),
    )
    .filter(F.col("num_suppliers") > 2)
)
