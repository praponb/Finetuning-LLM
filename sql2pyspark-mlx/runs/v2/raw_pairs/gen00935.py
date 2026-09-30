from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .select("id", "product_id", "quantity", F.rank().over(w).alias("quantity_rank"))
)
