from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price")

result = (
    df_order_items
    .filter(F.col("product_id") == 5)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.rank().over(w).alias("unit_price_rank"),
    )
)
