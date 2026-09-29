from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost", "id")

result = (
    df_shipments
    .select("id", "status", "cost", "order_id", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .select("id", "status", "cost", "order_id")
    .orderBy("order_id")
)
