from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .filter(F.col("account_id") == 37)
    .select("id", "merchant", "amount", "is_flagged", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "merchant", "amount", "is_flagged")
)
