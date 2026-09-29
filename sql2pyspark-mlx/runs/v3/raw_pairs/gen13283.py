from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.countDistinct("price").alias("unique_suppliers"))
)
