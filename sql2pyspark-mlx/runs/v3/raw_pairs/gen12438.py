from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")

result = (
    df_transactions
    .withColumn("max_amount_by_account", F.max("amount").over(w))
    .filter(F.col("amount") == F.col("max_amount_by_account"))
    .select("id", "account_id", "amount", "is_flagged")
)
