from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("merchant") != "Amazon")
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        F.lead("amount", 2).over(w).alias("next_amount"),
    )
)
