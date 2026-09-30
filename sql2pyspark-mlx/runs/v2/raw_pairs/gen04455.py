from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
        F.year("launch_date").alias("launch_year"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
    .filter(F.col("umbrella_count") > 5)
    .select("launch_month", "launch_year", "umbrella_count", "category_list")
    .orderBy("launch_month", "launch_year")
    .limit(10)
)
