from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isNull())
    .select("category")
    .unionByName(df_products.filter(F.col("stock_qty") <= 182).select("category"))
    .orderBy("category")
)
