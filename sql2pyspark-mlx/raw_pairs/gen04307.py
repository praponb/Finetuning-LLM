from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select("id", "order_id", "unit_price", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .orderBy(F.col("pos").desc())
)
