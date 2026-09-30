from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(680, 970) & (F.col("category") == "Home"))
    .orderBy(F.col("price").desc())
)
