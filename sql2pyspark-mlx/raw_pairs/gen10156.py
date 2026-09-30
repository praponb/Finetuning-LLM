from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("amount") > 1500)
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        F.lead("amount", 2, 0).over(w).alias("next_amount"),
    )
    .orderBy(F.col("next_amount").desc())
)
