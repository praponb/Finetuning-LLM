from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .select("id", "account_id", "amount", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .orderBy("account_id")
)
