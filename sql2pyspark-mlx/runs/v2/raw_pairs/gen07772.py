from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .filter(F.col("balance") < 66000)
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "account_type", "balance", "status")
    .orderBy(F.col("status").desc())
)
