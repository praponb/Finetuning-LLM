from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.size("tags") >= 1)
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"))
)
