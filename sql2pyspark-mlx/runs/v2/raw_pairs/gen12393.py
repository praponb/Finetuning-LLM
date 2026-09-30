from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity")

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select("id", "order_id", "quantity", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .orderBy(F.col("id").desc())
)
