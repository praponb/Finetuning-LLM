from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_products
    .filter(F.col("product_name").like("Co%"))
    .groupBy(
        F.when(F.col("cost") >= 790, "high").when(F.col("cost") >= 220, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum("price").alias("total_price"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_price") * 100) / F.sum("total_price").over(w)), 2))
    .orderBy(F.col("pct_of_total").desc(), "cost_bucket")
    .limit(3)
)
