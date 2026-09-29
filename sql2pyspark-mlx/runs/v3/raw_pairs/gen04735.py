from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.orderBy("quantity", "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.last("unit_price").over(w).alias("last_unit_price"),
        F.lead("unit_price", 1).over(w2).alias("next_unit_price"),
    )
)
