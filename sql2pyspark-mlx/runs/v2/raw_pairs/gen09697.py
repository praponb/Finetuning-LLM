from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Amazon", "7-Eleven"]))
    .select(
        "id",
        "txn_time",
        "amount",
        F.lead("amount", 1).over(w).alias("next_amount"),
    )
)
