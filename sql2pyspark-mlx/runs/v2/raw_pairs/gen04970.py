from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["UPS", "Kerry", "Flash"]))
    .select("id", "order_id", "cost", F.avg("cost").over(w).alias("order_id_avg_cost"))
    .orderBy(F.col("order_id_avg_cost").desc())
)
