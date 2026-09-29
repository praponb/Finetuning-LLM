from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("account_id", "merchant").orderBy("txn_time", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("merchant").orderBy("amount")
w5 = Window.partitionBy("account_id", "merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        F.dense_rank().over(w_ordered).alias("amount_dense_rank"),
        F.avg("amount").over(w2).alias("amount_avg_3_prior"),
        F.last("amount", ignorenulls=True).over(w3).alias("last_known_amount"),
        F.dense_rank().over(w_grp).alias("amount_dense_rank_2"),
        F.row_number().over(w5).alias("event_seq"),
    )
)
