from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@gmail.com"))
    .groupBy("is_active")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
    )
    .filter(F.col("singapore_count") >= 8)
    .select("is_active", "city_values", "singapore_count")
)
