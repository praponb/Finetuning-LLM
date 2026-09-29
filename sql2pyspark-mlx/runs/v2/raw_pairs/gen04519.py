from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 240)
    .groupBy("brand")
    .pivot("category", ["Home", "Sports", "Grocery"])
    .agg(F.max("price"))
    .orderBy("brand")
)
