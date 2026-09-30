from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_shipments
    .groupBy(
        F.when(F.col("status").isin(["LOST", "DELIVERED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(F.sum("cost").alias("total_cost"))
    .withColumn("pct_of_total", F.round(((F.col("total_cost") * 100) / F.sum("total_cost").over(w)), 2))
)
