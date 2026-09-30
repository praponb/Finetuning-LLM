from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount")

result = (
    df_transactions
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "account_id", "amount", "is_flagged")
    .orderBy(F.col("amount").desc())
)
