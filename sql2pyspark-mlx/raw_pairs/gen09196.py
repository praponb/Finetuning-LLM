from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .filter(F.col("customer_id") == 4)
    .select("id", "customer_id", "balance", F.rank().over(w).alias("balance_rank"))
    .orderBy(F.col("balance").desc())
)
