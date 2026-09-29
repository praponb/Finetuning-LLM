from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Home", "Sports", "Electronics", "Fashion"])
    .agg(F.count("cost"))
)
