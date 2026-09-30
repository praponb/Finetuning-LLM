from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("order_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .select(
        "*",
        F.round((F.col("unit_price") / F.sum("unit_price").over(w)), 4).alias("unit_price_share"),
        F.sum("unit_price").over(w2).alias("cumulative_unit_price"),
        F.avg("quantity").over(w3).alias("quantity_avg_3_prior"),
        F.row_number().over(w4).alias("event_seq"),
        F.row_number().over(w5).alias("row_seq"),
    )
)
