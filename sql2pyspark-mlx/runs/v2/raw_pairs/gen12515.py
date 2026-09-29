from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "SAVINGS", "LOAN"]))
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "account_type", "opened_date", "customer_id")
    .orderBy("opened_date", "id")
)
