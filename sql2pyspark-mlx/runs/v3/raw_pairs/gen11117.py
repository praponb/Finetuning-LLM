from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(
        F.count("*").alias("total_count"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .filter(F.col("total_count") > 1)
)
