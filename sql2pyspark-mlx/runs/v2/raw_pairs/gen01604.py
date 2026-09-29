from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        F.round((F.col("quantity") / F.sum("quantity").over(w)), 3).alias("quantity_share"),
        F.lead("quantity", 2).over(w2).alias("next_quantity"),
    )
)
