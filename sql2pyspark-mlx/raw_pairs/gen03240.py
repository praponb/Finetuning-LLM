from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .filter(F.col("quantity") > 5)
    .select("id", "order_id", "product_id", "quantity", F.rank().over(w).alias("rnk"))
)
