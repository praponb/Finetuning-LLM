from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_price").desc())

result = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .groupBy("category")
    .agg(
        F.sum("price").alias("total_price"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("total_price").desc(), F.col("cnt").desc())
)
