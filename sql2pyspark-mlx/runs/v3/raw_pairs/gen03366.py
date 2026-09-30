from pyspark.sql import functions as F

src = df_products.filter(F.col("category") == "Grocery")

summary = (
    src
    .groupBy("category")
    .agg(F.count("stock_qty").alias("stock_qty_count"))
)

result = summary
