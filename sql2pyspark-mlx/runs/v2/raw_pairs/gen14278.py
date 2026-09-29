from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date")

result = (
    df_accounts
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 1)
    .orderBy("id")
)
