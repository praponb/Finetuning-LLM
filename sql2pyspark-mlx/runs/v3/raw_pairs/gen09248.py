from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_month")

result = (
    df_shipments
    .filter(F.col("carrier") != "Flash")
    .groupBy(F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"))
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
)
