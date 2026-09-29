from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id")

result = (
    df_transactions
    .filter(F.col("amount") <= 1200)
    .select("id", "account_id", "amount", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
)
