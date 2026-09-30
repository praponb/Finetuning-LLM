from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Home", "Books", "Fashion"]) & F.col("price").between(650, 930))
    .groupBy("brand")
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
)
