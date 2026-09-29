from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .select("id", "product_id", "quantity", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .orderBy(F.col("rn").desc())
)
