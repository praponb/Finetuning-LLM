from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.count("*").alias("row_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
    .orderBy(F.col("row_count").desc())
)
