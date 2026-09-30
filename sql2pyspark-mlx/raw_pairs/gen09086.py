from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("order_id")

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.avg("quantity").over(w).alias("quantity_avg_3"),
        F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4).alias("quantity_share"),
    )
)
