from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
)
