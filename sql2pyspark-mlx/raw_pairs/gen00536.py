from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method", "status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("amount", "id")
w3 = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_payments
    .filter(F.col("method").isin(["paypal", "cash", "card"]))
    .select(
        "id",
        "order_id",
        "amount",
        F.sum("amount").over(w).alias("cumulative_amount"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
        F.ntile(4).over(w3).alias("amount_bucket"),
        F.sum("amount").over(w4).alias("cumulative_amount_2"),
    )
)

result = (
    metrics
    .select("id", "order_id", "next_amount", "amount_bucket", "cumulative_amount_2")
    .orderBy("id", "cumulative_amount_2")
)
