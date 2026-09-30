from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") != 274)
    .groupBy("brand")
    .pivot("category", ["Books", "Home", "Fashion"])
    .agg(F.count("price"))
    .orderBy("brand")
)
