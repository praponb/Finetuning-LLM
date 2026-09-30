from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("order_id") == 29)
    .select(
        "id",
        "order_id",
        "quantity",
        F.sum("quantity").over(w).alias("running_quantity"),
    )
)
