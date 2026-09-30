from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_group")

result = (
    df_shipments
    .groupBy(
        F.when(F.col("status").isin(["LOST", "IN_TRANSIT"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_cost", F.sum("total_cost").over(w))
)
