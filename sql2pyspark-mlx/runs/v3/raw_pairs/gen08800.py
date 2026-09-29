from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_price").desc())

result = (
    df_products
    .groupBy(
        F.when(F.col("category").isin(["Books", "Sports"]), "core").otherwise("other").alias("category_group"),
    )
    .agg(
        F.sum("price").alias("total_price"),
        F.count("*").alias("cnt"),
    )
    .withColumn("price_rank", F.dense_rank().over(w))
)
