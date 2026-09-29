from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.sum("discount").alias("total_discount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_discount") * 100) / F.sum("total_discount").over(w)), 2))
    .orderBy(F.col("total_discount").desc(), F.col("pct_of_total").desc())
)
