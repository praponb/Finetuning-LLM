from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Amazon", "7-Eleven"]))
    .select(
        "id",
        "merchant",
        "txn_time",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
)
