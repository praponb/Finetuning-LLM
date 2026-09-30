from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .select("id", "txn_type", "amount", F.row_number().over(w).alias("amount_row"))
)
