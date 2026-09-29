from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price").desc(), "id")
w3 = Window.partitionBy("order_id", "product_id")

result = (
    df_order_items
    .select(
        "*",
        F.corr("unit_price", "quantity").over(w).alias("corr_unit_price_quantity"),
        F.ntile(3).over(w2).alias("unit_price_bucket"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
    )
)
