from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .filter(F.col("quantity") <= 6)
    .select("id", "order_id", "quantity", F.ntile(10).over(w).alias("bucket"))
)
