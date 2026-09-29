from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("merchant") == "Starbucks")
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
    )
    .orderBy("id")
)
