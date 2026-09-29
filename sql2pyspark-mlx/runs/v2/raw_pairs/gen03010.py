from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("product_id").isin([29, 34]))
    .select(
        "id",
        "product_id",
        "unit_price",
        F.ntile(3).over(w).alias("unit_price_ntile"),
    )
)
