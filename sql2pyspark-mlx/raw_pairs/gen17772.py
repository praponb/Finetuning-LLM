from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .filter(~F.col("merchant").isin(["Grab", "Amazon"]))
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "merchant", "amount", "account_id")
)
