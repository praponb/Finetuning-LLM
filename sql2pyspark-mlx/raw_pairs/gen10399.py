from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .filter(F.col("quantity").between(5, 8))
    .select("id", "order_id", "quantity", F.row_number().over(w).alias("row_num"))
)
