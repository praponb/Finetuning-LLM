from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region").isin(["North", "West", "Central"]) & (F.col("opened_date") > "2025-09-01")) | F.col("store_name").like("R%"))
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
)
