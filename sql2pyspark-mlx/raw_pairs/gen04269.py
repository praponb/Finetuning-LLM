from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select("id", "order_id", "unit_price", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .orderBy("unit_price", "order_id")
)
