from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant")

result = (
    df_transactions
    .filter(F.hour("txn_time").between(6, 9))
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        "merchant",
        F.sum("amount").over(w).alias("running_amount"),
        F.min("amount").over(w2).alias("merchant_min_amount"),
    )
    .orderBy(F.col("running_amount").desc())
)
