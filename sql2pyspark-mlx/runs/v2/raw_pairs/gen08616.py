from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_shipments
    .withColumn("min_cost_by_status", F.min("cost").over(w))
    .filter(F.col("cost") == F.col("min_cost_by_status"))
    .select("id", "status", "cost", "shipped_date")
)
