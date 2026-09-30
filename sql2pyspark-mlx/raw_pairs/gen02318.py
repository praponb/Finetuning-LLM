from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_shipments
    .withColumn("avg_cost_by_status", F.avg("cost").over(w))
    .filter(F.col("cost") > F.col("avg_cost_by_status"))
    .select("id", "status", "cost")
    .orderBy(F.col("id").desc())
)
