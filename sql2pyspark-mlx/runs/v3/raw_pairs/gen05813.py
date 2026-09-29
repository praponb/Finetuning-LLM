from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").isNotNull() & F.array_contains("tags", "clearance"))
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
)
