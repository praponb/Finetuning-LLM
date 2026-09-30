from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date")

result = (
    df_accounts
    .filter(~F.col("status").isin(["FROZEN", "CLOSED"]))
    .select("id", "status", "opened_date", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "status", "opened_date")
    .orderBy("status")
)
