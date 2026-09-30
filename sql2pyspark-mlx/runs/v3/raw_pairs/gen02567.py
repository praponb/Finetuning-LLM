from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance")

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-10-01", "2023-12-21"))
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "account_type", "balance")
)
