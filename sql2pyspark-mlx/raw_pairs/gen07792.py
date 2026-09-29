from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") > 1160) | F.col("category").isin(["Grocery", "Sports", "Books"]))
    .groupBy(
        "supplier_id",
        F.when(F.col("brand").isin(["Initech", "Globex"]), "group_a").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
)
