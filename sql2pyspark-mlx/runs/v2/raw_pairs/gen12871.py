from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant")

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "amount",
        F.avg("amount").over(w).alias("merchant_avg_amount"),
    )
    .orderBy(F.col("merchant_avg_amount").desc(), F.col("amount").desc())
)
