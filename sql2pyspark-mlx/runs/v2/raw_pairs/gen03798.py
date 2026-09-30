from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        F.avg("amount").over(w).alias("account_id_avg_amount"),
        F.row_number().over(w2).alias("row_num"),
    )
)
