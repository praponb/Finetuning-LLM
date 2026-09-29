from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").isNotNull())
    .groupBy("brand")
    .pivot("category", ["Books", "Grocery", "Sports"])
    .agg(F.sum("price"))
)
