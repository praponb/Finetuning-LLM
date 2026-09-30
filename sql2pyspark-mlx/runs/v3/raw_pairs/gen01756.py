from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost")

result = (
    df_shipments
    .select("id", "order_id", "cost", "status", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "order_id", "cost", "status")
    .orderBy(F.col("status").desc())
)
