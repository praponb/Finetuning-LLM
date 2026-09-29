from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter((F.col("launch_date") >= "2025-05-01") | (F.size("tags") > 1))
    .withColumn("product_name_title", F.initcap(F.trim("product_name")))
)

agg = (
    filtered_products
    .groupBy(
        F.when(F.col("category").isin(["Sports", "Home"]), "tier_1").otherwise("other").alias("category_group"),
    )
    .agg(F.sort_array(F.collect_set("brand")).alias("brand_list"))
)

result = agg.orderBy("category_group")
