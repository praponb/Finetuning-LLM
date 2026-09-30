from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance")

result = (
    df_accounts
    .select("id", "status", "balance", "opened_date", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .orderBy("id")
)
