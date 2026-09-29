from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "FEE"]))
    .select(
        "id",
        "merchant",
        "txn_time",
        "amount",
        F.lead("amount", 1).over(w).alias("next_amount"),
    )
    .orderBy("merchant")
)
