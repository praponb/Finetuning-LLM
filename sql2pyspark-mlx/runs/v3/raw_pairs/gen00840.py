from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .groupBy("brand")
    .pivot("category", ["Electronics", "Fashion"])
    .agg(F.sum("price"))
)
