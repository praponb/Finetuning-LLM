from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "account_id", "amount")
    .orderBy(F.col("id").desc())
)
