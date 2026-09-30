from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .select("id", "status", "balance", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "status", "balance")
)
