from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .select("id", "account_id", "amount", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "account_id", "amount")
    .orderBy("amount")
)
