from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("order_date"), "status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "status").orderBy("shipping_fee", "id")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "discount",
        F.sum("discount").over(w).alias("running_discount"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
