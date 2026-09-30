from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_products
    .groupBy(
        F.when(F.col("price") >= 980, "high").when(F.col("price") >= 550, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(F.sum("price").alias("total_price"))
    .withColumn("pct_of_total", F.round(((F.col("total_price") * 100) / F.sum("total_price").over(w)), 2))
    .orderBy("price_bucket", F.col("total_price").desc())
)
