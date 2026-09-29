from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_order_items
    .filter(F.col("order_id").isin([10, 11, 30, 33]))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.round(F.avg("unit_price").over(w), 2).alias("unit_price_moving_avg_4"),
    )
    .orderBy(F.col("quantity").desc())
)
