from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.min("price").alias("min_price"),
    )
)
