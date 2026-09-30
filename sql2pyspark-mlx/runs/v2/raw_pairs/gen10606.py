from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("merchant")

result = (
    df_transactions
    .filter(~F.col("txn_type").isin(["CREDIT", "REFUND"]))
    .select(
        "id",
        "merchant",
        "amount",
        F.rank().over(w).alias("amount_rank"),
        F.max("amount").over(w2).alias("merchant_max_amount"),
    )
)
