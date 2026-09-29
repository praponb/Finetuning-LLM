from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_order_items
    .filter(F.col("unit_price") <= 330)
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity",
        F.round((F.col("unit_price") / F.sum("unit_price").over(w)), 3).alias("unit_price_share"),
        (F.col("quantity") - F.lag("quantity", 1).over(w2)).alias("quantity_change"),
    )
    .orderBy("unit_price")
)
