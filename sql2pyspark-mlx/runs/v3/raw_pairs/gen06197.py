from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id")

result = (
    df_order_items
    .select("id", "unit_price", F.row_number().over(w).alias("unit_price_row"))
    .orderBy(F.col("unit_price").desc())
)
