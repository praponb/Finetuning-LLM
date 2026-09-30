from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .crossJoin(df_products.alias("p"))
    .select(F.col("s.region"), F.col("p.category"))
)
