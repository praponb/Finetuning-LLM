from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("order_id").isin([17, 23, 38]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.count("*").over(w).alias("order_id_count"),
        F.row_number().over(w2).alias("rn"),
    )
)
