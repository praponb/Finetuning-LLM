from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy(F.year("txn_time"), "account_id").orderBy("amount", "id").rowsBetween(-5, -1)
w2 = Window.orderBy(F.col("amount").desc_nulls_last())
w3 = Window.partitionBy("account_id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.avg("amount").over(w_ordered).alias("amount_avg_5_prior"),
        F.rank().over(w2).alias("amount_rank"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)
