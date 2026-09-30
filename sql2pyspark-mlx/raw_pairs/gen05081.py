from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
)
