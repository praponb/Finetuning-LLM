from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance")

result = (
    df_accounts
    .select("id", "status", "balance", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "status", "balance")
    .orderBy(F.col("balance").desc())
)
