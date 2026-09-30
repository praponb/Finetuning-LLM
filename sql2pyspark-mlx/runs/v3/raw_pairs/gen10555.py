from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 37)
    .rollup(
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
        F.when(F.col("brand").isin(["Initech", "Acme"]), "tier_1").otherwise("other").alias("brand_group"),
    )
    .agg(F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"))
    .orderBy("launch_month")
)
