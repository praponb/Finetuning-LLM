from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "order_id").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("status", "order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("paid_at"), "method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_payments
    .select(
        "*",
        F.first("method", ignorenulls=True).over(w).alias("first_known_method"),
        F.row_number().over(w2).alias("seq_no"),
        F.sum("amount").over(w3).alias("running_amount"),
        F.first("method", ignorenulls=True).over(w4).alias("first_known_method_2"),
    )
)
