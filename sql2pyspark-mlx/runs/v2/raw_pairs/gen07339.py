from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_month")

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.sum("units").alias("total_units"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_units", F.sum("total_units").over(w))
    .orderBy(F.col("cumulative_units").desc(), F.col("cnt").desc())
)
