from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipped_year")

result = (
    df_shipments
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
)
