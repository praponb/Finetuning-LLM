from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Home", "Grocery", "Sports"])
    .agg(F.max("price"))
)
