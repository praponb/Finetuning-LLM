from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .filter(F.col("balance").between(24000, 52000))
    .select("id", "status", "balance", F.rank().over(w).alias("balance_rank"))
)
