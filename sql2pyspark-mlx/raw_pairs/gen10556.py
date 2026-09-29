from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("opened_date").desc())

ranked = (
    df_accounts
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "balance", "opened_date", "status")
)

result = (
    ranked
    .select("id", "opened_date", "status")
    .orderBy(F.col("opened_date").asc_nulls_first())
)
