from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc())

result = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        F.count("*").over(w).alias("method_count"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
    )
)
