from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("quantity").between(4, 7))
    .select(
        "id",
        "product_id",
        "quantity",
        "order_id",
        F.sum("quantity").over(w).alias("product_id_sum_quantity"),
        F.lead("quantity", 2).over(w2).alias("next_quantity"),
    )
)
