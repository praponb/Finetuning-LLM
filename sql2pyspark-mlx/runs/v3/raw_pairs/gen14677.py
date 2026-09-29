from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .select("id", "account_type", "balance", F.rank().over(w).alias("rnk"))
    .orderBy("id")
)
