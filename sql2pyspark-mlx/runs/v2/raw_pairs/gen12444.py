from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2024-12-11")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "account_type", "balance")
    .orderBy(F.col("id").desc())
)
