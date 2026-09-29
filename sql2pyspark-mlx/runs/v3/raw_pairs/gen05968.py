from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([22, 31, 33, 35]))
    .select(
        "id",
        "merchant",
        "amount",
        "account_id",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "merchant", "amount", "account_id")
)
