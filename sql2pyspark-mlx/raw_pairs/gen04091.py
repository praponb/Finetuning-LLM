from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance")

result = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .select("id", "customer_id", "balance", F.dense_rank().over(w).alias("dense_rnk"))
)
