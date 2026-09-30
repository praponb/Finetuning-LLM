from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("quantity") > 3)
    .select(
        "id",
        "unit_price",
        "order_id",
        F.dense_rank().over(w).alias("unit_price_dense_rank"),
        F.lead("unit_price", 2, 0).over(w2).alias("next_unit_price"),
    )
)
