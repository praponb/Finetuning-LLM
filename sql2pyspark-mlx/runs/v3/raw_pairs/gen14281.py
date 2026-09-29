from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 2)
)
