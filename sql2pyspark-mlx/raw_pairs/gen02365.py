from pyspark.sql import functions as F

valid_products = df_products.filter(F.col("brand").isin(["Stark", "Initech"]))

agg = (
    valid_products
    .groupBy(
        "category",
        F.when(F.col("brand").isin(["Globex", "Stark"]), "group_a").otherwise("other").alias("brand_group"),
    )
    .agg(F.sort_array(F.collect_set("category")).alias("category_list"))
)

result = (
    agg
    .select("category", "brand_group", "category_list")
    .orderBy("category", "brand_group")
    .limit(5)
)
