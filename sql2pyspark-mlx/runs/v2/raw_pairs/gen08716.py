from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")

result = (
    df_transactions
    .withColumn("avg_amount_by_account", F.avg("amount").over(w))
    .filter(F.col("amount") > F.col("avg_amount_by_account"))
    .select("id", "account_id", "amount")
    .orderBy(F.col("amount").desc())
)
