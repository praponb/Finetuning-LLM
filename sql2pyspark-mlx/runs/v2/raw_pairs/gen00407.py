from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "txn_time",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
    )
)
