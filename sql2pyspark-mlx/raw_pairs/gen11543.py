from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Globex", "Umbrella"]))
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.count("cost").alias("cost_count"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .filter(F.col("cost_count") > 8)
    .select("launch_year", "cost_count", "brand_list")
)
