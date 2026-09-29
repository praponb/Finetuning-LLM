from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id")
w2 = Window.orderBy(F.col("amount").desc())

result = (
    df_transactions
    .filter(F.col("merchant") != "Amazon")
    .select(
        "id",
        "account_id",
        "amount",
        F.row_number().over(w).alias("amount_row"),
        F.rank().over(w2).alias("amount_rank"),
    )
)
