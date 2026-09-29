from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("quantity").desc(), "id")
w_part = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w_hist = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "unit_price",
        F.row_number().over(w_grp).alias("event_seq"),
        F.last("quantity", ignorenulls=True).over(w_part).alias("last_known_quantity"),
        F.avg("unit_price").over(w3).alias("unit_price_avg_6"),
        F.ntile(3).over(w_hist).alias("unit_price_bucket"),
        F.ntile(5).over(w_grp).alias("quantity_bucket"),
    )
)
