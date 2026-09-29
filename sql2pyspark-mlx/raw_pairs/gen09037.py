from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") <= 460)
    .groupBy("category")
    .agg(
        F.count("cost").alias("cost_count"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .filter(F.col("cost_count") > 4)
    .orderBy(F.col("category").desc())
)
