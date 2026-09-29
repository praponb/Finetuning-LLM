from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_order_items
    .filter(F.col("quantity") < 7)
    .select("id", "order_id", F.count("*").over(w).alias("order_id_count"))
)
