from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_orders
    .filter(F.col("customer_id") == 11)
    .select(
        "id",
        "store_id",
        "order_date",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
)
