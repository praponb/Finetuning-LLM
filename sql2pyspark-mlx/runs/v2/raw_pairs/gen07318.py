from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        F.rank().over(w).alias("rnk"),
        F.lead("quantity", 2).over(w2).alias("next_quantity"),
    )
)
