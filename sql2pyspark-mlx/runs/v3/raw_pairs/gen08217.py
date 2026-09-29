from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("merchant").orderBy("amount")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        F.rank().over(w).alias("amount_rank"),
        F.rank().over(w2).alias("rnk"),
    )
)
