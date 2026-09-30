from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.lower(F.trim("product_name")) == "backpack xl") | F.array_contains("tags", "sale"))
    .groupBy("brand", "category")
    .agg(
        F.max("cost").alias("max_cost"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
)
