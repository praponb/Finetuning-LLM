from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.hour("txn_time").between(12, 16))
    .select(
        "id",
        "account_id",
        "amount",
        "is_flagged",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 1)
    .select("id", "account_id", "amount", "is_flagged")
    .orderBy(F.col("amount").desc())
)
