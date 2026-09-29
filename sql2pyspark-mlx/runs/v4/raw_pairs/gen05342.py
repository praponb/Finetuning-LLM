from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 6, 8, 10]))
    .groupBy(
        F.when(F.col("revenue") >= 4400, "high").when(F.col("revenue") >= 1300, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.sum("revenue").alias("total_revenue"))
    .withColumn("pct_of_total", F.round(((F.col("total_revenue") * 100) / F.sum("total_revenue").over(w)), 2))
    .orderBy("revenue_bucket")
    .limit(1)
)
