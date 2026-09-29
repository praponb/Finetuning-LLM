from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-11-01")
    .select(
        "id",
        "status",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share_2"),
    )
)
