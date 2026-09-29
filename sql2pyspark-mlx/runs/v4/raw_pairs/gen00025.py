from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 3)
)
