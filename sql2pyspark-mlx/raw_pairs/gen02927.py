from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("paid_at"), "method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("amount").asc_nulls_first())
w3 = Window.partitionBy("method", "order_id").orderBy("amount", "id")

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "method",
        "status",
        F.sum("amount").over(w).alias("running_amount"),
        F.rank().over(w2).alias("amount_rank"),
        F.lead("amount", 1).over(w3).alias("next_amount"),
    )
)
