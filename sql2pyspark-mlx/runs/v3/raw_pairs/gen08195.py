from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance")

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
    .select("id", "status", "balance", "customer_id", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "status", "balance", "customer_id")
    .orderBy("id")
)
