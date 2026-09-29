from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([7, 31, 35]))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "account_id", "amount", "txn_time")
    .orderBy(F.col("id").desc())
)
