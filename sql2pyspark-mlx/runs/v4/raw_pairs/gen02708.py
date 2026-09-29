from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.col("merchant") != "Grab")
    .select(
        "id",
        "merchant",
        "txn_time",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
        F.row_number().over(w2).alias("amount_row"),
    )
)
