from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([8, 16, 18, 34]))
    .select("id", "txn_type", "amount", F.row_number().over(w).alias("rn"))
)
