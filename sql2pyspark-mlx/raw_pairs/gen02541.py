from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w_grp = Window.orderBy(F.col("quantity").desc(), "id")

enriched = (
    df_order_items
    .select(
        "*",
        F.ntile(3).over(w).alias("unit_price_bucket"),
        F.ntile(5).over(w_grp).alias("quantity_bucket"),
    )
)

result = (
    enriched
    .select("id", "product_id", "quantity", "unit_price", "unit_price_bucket")
    .orderBy(F.col("quantity").desc(), F.col("unit_price_bucket").desc())
)
