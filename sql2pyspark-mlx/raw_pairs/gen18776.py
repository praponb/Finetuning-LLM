from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method", "order_id").orderBy("paid_at", "id")

with_windows = (
    df_payments
    .select(
        "*",
        F.count("amount").over(w_ordered).alias("running_count_amount"),
        F.avg("amount").over(w_ordered).alias("running_avg_amount"),
        F.min("amount").over(w_ordered).alias("running_min_amount"),
        F.lead("amount", 2).over(w2).alias("next_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "running_avg_amount",
        "running_min_amount",
        F.when(F.col("method").rlike("^(card|promptpay)$"), "PRIORITY").otherwise("GROUP_A").alias("method_zone"),
    )
    .orderBy(F.col("amount").asc_nulls_first(), F.col("status").desc())
)
