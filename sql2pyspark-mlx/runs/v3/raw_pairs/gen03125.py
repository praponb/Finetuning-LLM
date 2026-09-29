from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")

result = (
    df_order_items
    .select("id", "order_id", "quantity", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .select("id", "order_id", "quantity")
    .orderBy(F.col("order_id").desc())
)
