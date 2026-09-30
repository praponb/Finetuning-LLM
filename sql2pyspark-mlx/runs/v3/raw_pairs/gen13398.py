from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
        F.ntile(3).over(w2).alias("bucket"),
    )
)
