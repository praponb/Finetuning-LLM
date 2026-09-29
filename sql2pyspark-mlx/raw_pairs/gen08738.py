from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy(F.col("amount").asc_nulls_first())

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "txn_type",
        "amount",
        "is_flagged",
        F.lag("amount").over(w).alias("prev_amount"),
        F.row_number().over(w).alias("row_seq"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
    )
)
