from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .orderBy("category_values", "supplier_id")
    .limit(20)
)
