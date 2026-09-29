from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_payments
    .filter(F.col("amount").between(470, 1570))
    .select(
        "id",
        "status",
        "amount",
        F.avg("amount").over(w).alias("status_avg_amount"),
    )
    .orderBy(F.col("status").desc())
)
