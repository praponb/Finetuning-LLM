from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("account_id").orderBy("amount", "id")
w2 = Window.partitionBy("txn_type", "merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("account_id") == 17)
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        F.lead("amount").over(w_ordered).alias("next_amount"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.lead("amount", 1).over(w2).alias("next_amount_2"),
    )
)
