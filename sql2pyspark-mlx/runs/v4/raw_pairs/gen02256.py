from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy("amount", "id")

result = (
    df_payments
    .select(
        "id",
        "status",
        "method",
        "amount",
        F.ntile(2).over(w).alias("amount_ntile"),
    )
    .orderBy(F.col("amount").desc())
)
