from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity")

result = (
    df_order_items
    .filter(F.col("unit_price").between(150, 310))
    .select("id", "order_id", "quantity", F.rank().over(w).alias("rnk"))
    .orderBy("id")
)
