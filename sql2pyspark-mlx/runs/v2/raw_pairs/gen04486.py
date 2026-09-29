from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_year")

result = (
    df_shipments
    .groupBy(F.year("delivered_date").alias("delivered_year"))
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
)
