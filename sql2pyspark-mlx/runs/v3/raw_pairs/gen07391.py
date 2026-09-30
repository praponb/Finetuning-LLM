from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("merchant").orderBy("amount", "id")

result = (
    df_transactions
    .filter(F.col("amount") < 3500)
    .select(
        "id",
        "merchant",
        "amount",
        "account_id",
        F.first("account_id").over(w).alias("first_account_id"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy(F.col("id").desc())
)
