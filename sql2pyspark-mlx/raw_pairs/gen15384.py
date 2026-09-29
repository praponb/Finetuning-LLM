from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("txn_time"), "merchant")
w2 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.orderBy("amount", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.max("amount").over(w2).alias("amount_max_30d"),
        F.avg("amount").over(w3).alias("amount_avg_3_prior"),
        F.row_number().over(w4).alias("seq_no"),
    )
)
