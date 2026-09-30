from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") < 440) & F.col("cost").isNotNull() & (F.col("stock_qty") <= 216))
    .select("id", "brand")
    .orderBy("brand", "id")
    .limit(5)
)
