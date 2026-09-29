from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant")

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "amount",
        F.sum("amount").over(w).alias("merchant_sum_amount"),
    )
    .orderBy("merchant")
)
