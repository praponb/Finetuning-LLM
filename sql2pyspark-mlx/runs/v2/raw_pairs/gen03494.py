from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "status", "opened_date", "balance")
    .orderBy(F.col("balance").desc())
)
