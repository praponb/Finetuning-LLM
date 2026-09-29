from pyspark.sql import functions as F

base = df_products.filter(F.array_contains("tags", "eco"))

agg = (
    base
    .groupBy("brand")
    .agg(F.countDistinct("price").alias("unique_categorys"))
)

result = agg.orderBy("brand")
