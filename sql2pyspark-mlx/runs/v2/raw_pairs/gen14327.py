from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") >= 300)
    .groupBy("brand")
    .pivot("category", ["Home", "Sports", "Fashion", "Electronics"])
    .agg(F.avg("price"))
)
