from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("txn_time"), "txn_type").orderBy("amount", "id")
w2 = Window.partitionBy("account_id")

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "is_flagged",
        F.lead("amount", 1).over(w).alias("next_amount"),
        F.min("amount").over(w2).alias("account_id_min_amount"),
    )
)
