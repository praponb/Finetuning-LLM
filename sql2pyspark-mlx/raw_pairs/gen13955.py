from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "supplier_id",
        F.when(F.col("brand").isin(["Acme", "Stark"]), "priority").otherwise("other").alias("brand_group"),
        "category",
    )
    .agg(
        F.max("price").alias("max_price"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.min("cost").alias("min_cost"),
    )
)
