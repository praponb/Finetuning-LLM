from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        F.round((F.col("unit_price") / F.sum("unit_price").over(w)), 3).alias("unit_price_share"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w)), 3).alias("unit_price_share_2"),
    )
)
