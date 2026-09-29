from pyspark.sql import functions as F

result = (
    df_products
    .select("stock_qty", "tags", "cost")
    .orderBy(F.col("cost").desc())
)
