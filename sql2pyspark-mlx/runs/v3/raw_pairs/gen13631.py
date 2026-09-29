from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("supplier_id") == 23) | F.col("supplier_id").isin([26, 30, 33]))
    .groupBy("brand", "category")
    .agg(
        F.countDistinct("price").alias("unique_categorys"),
        F.count("price").alias("price_count"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
)
