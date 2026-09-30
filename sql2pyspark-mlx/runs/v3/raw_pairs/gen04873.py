from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .filter(F.col("unit_price") >= 230)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "product_id",
        F.lead("unit_price", 1).over(w).alias("next_unit_price"),
        F.ntile(10).over(w2).alias("bucket"),
    )
)
