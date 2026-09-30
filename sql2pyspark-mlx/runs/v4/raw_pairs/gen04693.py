from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([2, 7, 12]))
    .groupBy(
        F.when(F.col("brand").isin(["Umbrella", "Acme"]), "core").otherwise("other").alias("brand_group"),
        "category",
        "brand",
    )
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.max("launch_date").alias("latest_launch_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
    .orderBy(F.col("brand_group").desc(), F.col("brand").desc())
)
