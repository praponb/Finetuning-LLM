from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "account_type", "opened_date")
    .orderBy("id")
)
