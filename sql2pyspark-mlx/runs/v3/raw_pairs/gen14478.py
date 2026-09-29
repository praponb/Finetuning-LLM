from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(330, 1030))
    .groupBy(
        F.year("launch_date").alias("launch_year"),
        F.when(F.col("brand").isin(["Umbrella", "Globex"]), "tier_1").otherwise("other").alias("brand_group"),
        "brand",
    )
    .agg(
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
    .orderBy("brand_group")
)
