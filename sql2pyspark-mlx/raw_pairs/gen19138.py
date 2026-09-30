from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy("amount", "id")
w2 = Window.partitionBy(F.to_date("paid_at")).orderBy(F.col("amount").desc_nulls_first())

result = (
    df_payments
    .filter(F.col("amount") > 840)
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.lead("amount", 2).over(w).alias("next_amount"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
    )
)
