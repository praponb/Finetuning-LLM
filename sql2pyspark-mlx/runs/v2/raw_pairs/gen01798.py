from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("quantity") <= 5)
    .select("id", "unit_price", F.lag("unit_price", 1).over(w).alias("prev_unit_price"))
)
