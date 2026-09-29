from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_shipments
    .select("id", "order_id", F.count("*").over(w).alias("order_id_count"))
    .orderBy(F.col("id").desc())
)
