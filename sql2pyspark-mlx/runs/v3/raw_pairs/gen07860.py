from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.sum("price").alias("total_price"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_price") * 100) / F.sum("total_price").over(w)), 2))
    .orderBy(F.col("pct_of_total").desc(), F.col("launch_quarter").desc())
)
