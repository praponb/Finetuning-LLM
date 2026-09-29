from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("order_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.round((F.col("quantity") / F.sum("quantity").over(w2)), 3).alias("quantity_share"),
    )
)
