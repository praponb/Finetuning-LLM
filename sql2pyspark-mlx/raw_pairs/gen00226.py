from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") > 163) & (F.lower(F.trim("product_name")) == "backpack xl"))
    .groupBy("brand", "category")
    .agg(F.sort_array(F.collect_set("brand")).alias("brand_list"))
    .orderBy("category", "brand")
    .limit(10)
)
