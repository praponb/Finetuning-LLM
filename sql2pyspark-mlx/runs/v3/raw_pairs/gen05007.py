from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        F.lead("amount", 2).over(w).alias("next_amount"),
        F.rank().over(w2).alias("rnk"),
    )
)
