from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("order_date"), "store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .filter(F.col("channel") == "web")
    .select(
        "id",
        "order_date",
        "store_id",
        "shipping_fee",
        F.sum("shipping_fee").over(w).alias("running_shipping_fee"),
    )
)
