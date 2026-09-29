from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc())
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        "quantity",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.sum("quantity").over(w2).alias("running_quantity"),
    )
)
