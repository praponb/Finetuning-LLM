from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter((F.col("stock_qty") >= 256) & (F.col("launch_date") > "2023-11-05"))
)

agg = (
    filtered_products
    .groupBy("category")
    .agg(F.sort_array(F.collect_set("brand")).alias("brand_list"))
)

result = agg.orderBy(F.col("category").desc())
