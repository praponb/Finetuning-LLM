from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%OKY%"))
    .groupBy("country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
    )
    .filter(F.col("us_count") > 3)
)
