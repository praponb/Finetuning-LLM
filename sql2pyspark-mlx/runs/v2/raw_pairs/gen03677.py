from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.sort_array(F.collect_set("category")).alias("category_list"))
    .orderBy("category")
)
