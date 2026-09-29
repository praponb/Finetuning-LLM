from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.col("txn_time") < "2025-02-01")
    .select("id", "amount", F.row_number().over(w).alias("rn"))
)
