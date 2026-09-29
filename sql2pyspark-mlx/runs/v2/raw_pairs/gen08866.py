from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.max("price").alias("max_price"),
    )
    .filter(F.col("max_price") > 560)
    .orderBy(F.col("max_price").desc())
)
