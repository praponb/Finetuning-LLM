from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-10-12", "2024-12-01"))
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 1)
    .select("id", "account_type", "balance", "opened_date")
)
