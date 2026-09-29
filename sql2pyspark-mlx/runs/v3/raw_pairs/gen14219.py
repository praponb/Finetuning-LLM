from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost")

result = (
    df_shipments
    .select("id", "order_id", "cost", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "cost")
)
