from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("status", "method").orderBy("paid_at", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_payments
    .filter(F.hour("paid_at").between(7, 14))
    .select(
        "id",
        "amount",
        "status",
        F.max("amount").over(w_ordered).alias("running_max_amount"),
        F.count("amount").over(w_ordered).alias("running_count_amount"),
        F.min("amount").over(w_ordered).alias("running_min_amount"),
        F.avg("amount").over(w_part).alias("amount_avg_5"),
    )
)
