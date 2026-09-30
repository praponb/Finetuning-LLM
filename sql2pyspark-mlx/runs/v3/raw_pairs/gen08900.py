from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date")

result = (
    df_accounts
    .filter(F.month("opened_date").isin([1, 4, 6]))
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "status", "opened_date", "customer_id")
    .orderBy(F.col("customer_id").desc())
)
