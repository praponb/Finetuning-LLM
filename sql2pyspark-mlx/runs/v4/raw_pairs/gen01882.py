from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method", "status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

result = (
    df_payments
    .select(
        "*",
        F.sum("amount").over(w).alias("cumulative_amount"),
        F.sum("amount").over(w2).alias("amount_sum_7d"),
    )
)
