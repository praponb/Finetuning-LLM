from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.countDistinct("price").alias("unique_categorys"),
    )
    .filter(F.col("price_range") < 25)
)
