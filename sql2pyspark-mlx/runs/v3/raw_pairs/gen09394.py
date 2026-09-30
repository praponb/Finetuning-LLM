from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Electronics", "Sports", "Grocery"]))
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.countDistinct("stock_qty").alias("unique_brands"),
    )
)
