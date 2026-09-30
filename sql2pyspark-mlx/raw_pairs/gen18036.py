from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") == "Home")
    .rollup("supplier_id", "brand")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.countDistinct("cost").alias("unique_brands"),
    )
    .orderBy("unique_brands")
)
