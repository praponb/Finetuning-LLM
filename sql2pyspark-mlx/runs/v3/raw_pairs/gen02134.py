from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        (F.max("price") - F.min("price")).alias("price_range"),
    )
    .filter(F.col("price_range") > 72)
    .orderBy(F.col("category_values").desc(), "price_range", "launch_year")
    .limit(1)
)
