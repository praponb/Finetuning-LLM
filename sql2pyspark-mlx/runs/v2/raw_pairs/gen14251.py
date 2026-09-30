from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(F.sort_array(F.collect_set("category")).alias("category_list"))
    .orderBy("supplier_id")
    .limit(20)
)
