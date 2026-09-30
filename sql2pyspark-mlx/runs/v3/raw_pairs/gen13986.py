from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([2, 23, 25, 32]) & (F.col("stock_qty") <= 207) & F.col("category").isin(["Books", "Sports", "Grocery"]))
    .groupBy("category")
    .agg(F.countDistinct("stock_qty").alias("unique_categorys"))
    .orderBy(F.col("unique_categorys").desc(), "category")
)
