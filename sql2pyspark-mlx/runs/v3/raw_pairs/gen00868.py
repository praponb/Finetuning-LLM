from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .filter(F.col("amount") >= 3000)
    .select("id", "txn_type", "amount", F.rank().over(w).alias("amount_rank"))
)
