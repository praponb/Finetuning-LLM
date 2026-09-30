from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id")
w4 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "quantity",
        "unit_price",
        F.avg("unit_price").over(w).alias("unit_price_avg_2_prior"),
        F.last("unit_price", ignorenulls=True).over(w2).alias("last_known_unit_price"),
        F.corr("quantity", "unit_price").over(w3).alias("corr_quantity_unit_price"),
        F.sum("unit_price").over(w4).alias("cumulative_unit_price"),
    )
)
