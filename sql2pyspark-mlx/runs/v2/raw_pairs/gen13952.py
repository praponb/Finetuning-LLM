from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .filter(F.col("amount").between(2900, 3100))
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
    .orderBy("txn_type")
)
