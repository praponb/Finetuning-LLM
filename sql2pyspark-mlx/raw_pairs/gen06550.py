from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 2)
)
