from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "sale") & (F.col("stock_qty") != 197))
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.max("price").alias("max_price"),
    )
    .orderBy("brand_values")
)
