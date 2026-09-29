from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_shipments
    .filter(F.col("carrier") != "FedEx")
    .select("id", "order_id", F.count("*").over(w).alias("order_id_count"))
)
