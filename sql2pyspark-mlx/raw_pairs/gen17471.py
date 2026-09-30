from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("id", "account_id", "amount", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 1)
    .select("id", "account_id", "amount")
    .orderBy(F.col("amount").desc())
)
