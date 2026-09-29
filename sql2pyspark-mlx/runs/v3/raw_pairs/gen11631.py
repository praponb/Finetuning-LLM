from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id")

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "account_id",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
    )
    .orderBy("id")
)
