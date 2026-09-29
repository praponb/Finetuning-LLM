from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Electronics", "Grocery"])
    .agg(F.avg("price"))
    .orderBy("brand")
)
