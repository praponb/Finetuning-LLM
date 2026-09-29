from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .cube(
        F.month("launch_date").alias("launch_month"),
        F.when(F.col("stock_qty") >= 375, "high").when(F.col("stock_qty") >= 72, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
    .orderBy("launch_month", F.col("category_values").desc())
)
