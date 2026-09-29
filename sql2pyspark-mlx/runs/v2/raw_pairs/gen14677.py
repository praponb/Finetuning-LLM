from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id")

result = (
    df_transactions
    .select("id", "merchant", "amount", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "merchant", "amount")
)
