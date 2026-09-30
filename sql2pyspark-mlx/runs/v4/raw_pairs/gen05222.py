from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id")

result = (
    df_transactions
    .filter(F.col("txn_type") == "REFUND")
    .select("id", "account_id", "amount", F.row_number().over(w).alias("rn"))
)
