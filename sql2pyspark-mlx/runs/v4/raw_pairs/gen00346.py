from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "account_id", "amount")
    .orderBy(F.col("account_id").desc())
)
