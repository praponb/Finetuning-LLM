from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("supplier_id") == 17)
    .withColumn("stock_qty_per_cost", F.round((F.col("stock_qty") / F.col("cost")), 2))
)

summary = (
    recent_products
    .groupBy("category")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
)

result = summary
