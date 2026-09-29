from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date")

result = (
    df_accounts
    .filter(F.col("customer_id") == 40)
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "customer_id", "opened_date", "status")
)
