from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("price_bucket")

result = (
    df_products
    .filter(F.col("brand").isin(["Acme", "Initech"]))
    .groupBy(
        F.when(F.col("price") >= 920, "high").when(F.col("price") >= 450, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
)
