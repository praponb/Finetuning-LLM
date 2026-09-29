from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .select("id", "account_id", "amount", "merchant")
    .orderBy("amount")
)
