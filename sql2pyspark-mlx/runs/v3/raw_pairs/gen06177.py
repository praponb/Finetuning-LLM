from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.col("account_id").isin([1, 15, 37]))
    .select("id", "merchant", "amount", F.row_number().over(w).alias("rn"))
)
