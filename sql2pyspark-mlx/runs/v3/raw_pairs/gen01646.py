from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant")

result = (
    df_transactions
    .filter(F.col("account_id").isin([7, 8, 16, 21]))
    .select(
        "id",
        "merchant",
        "amount",
        F.round((F.col("amount") / F.sum("amount").over(w)), 3).alias("amount_share"),
    )
)
