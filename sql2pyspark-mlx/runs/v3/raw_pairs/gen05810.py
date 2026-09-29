from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("unit_price")

result = (
    df_order_items
    .filter(F.col("order_id") == 24)
    .select(
        "id",
        "product_id",
        "order_id",
        "unit_price",
        F.rank().over(w).alias("unit_price_rank"),
    )
)
