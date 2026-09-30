from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_orders
    .filter(F.col("order_date") <= "2023-06-01")
    .groupBy(
        F.when(F.col("discount") >= 0.36, "high").when(F.col("discount") >= 0.09, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.sum("discount").alias("total_discount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_discount") * 100) / F.sum("total_discount").over(w)), 2))
)
