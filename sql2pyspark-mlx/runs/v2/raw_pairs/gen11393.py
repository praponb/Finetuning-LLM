from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date")

result = (
    df_accounts
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 2)
    .select("id", "status", "opened_date", "customer_id")
)
