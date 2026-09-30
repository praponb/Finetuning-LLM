from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_payments
    .filter(F.col("status") != "FAILED")
    .select(
        "id",
        "order_id",
        F.count("amount").over(w).alias("amount_count_90d"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
    )
)

result = enriched.select("id", "amount_count_90d", "cumulative_amount")
