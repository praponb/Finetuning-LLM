from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_shipments
    .select("id", "status", "cost", F.max("cost").over(w).alias("status_max_cost"))
)
