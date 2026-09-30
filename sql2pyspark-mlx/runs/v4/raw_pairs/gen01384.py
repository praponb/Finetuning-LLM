from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id")
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_type",
        F.ntile(10).over(w).alias("amount_ntile"),
        F.row_number().over(w2).alias("rn"),
    )
)
