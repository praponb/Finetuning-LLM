from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance")

result = (
    df_accounts
    .filter(F.col("balance") < 25000)
    .select("id", "account_type", "balance", F.rank().over(w).alias("rnk"))
)
