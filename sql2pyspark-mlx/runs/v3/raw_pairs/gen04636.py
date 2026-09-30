from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_shipments
    .filter(F.col("status") != "LOST")
    .select("id", "order_id", "cost", F.max("cost").over(w).alias("order_id_max_cost"))
    .orderBy("cost")
)
