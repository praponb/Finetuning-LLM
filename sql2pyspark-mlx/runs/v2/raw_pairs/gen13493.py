from pyspark.sql import functions as F

src = df_products.filter(F.col("stock_qty").between(263, 289))

agg = (
    src
    .groupBy("category")
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.sum("cost").alias("total_cost"),
    )
)

result = (
    agg
    .select("category", "min_stock_qty", "total_cost")
    .orderBy("category")
)
