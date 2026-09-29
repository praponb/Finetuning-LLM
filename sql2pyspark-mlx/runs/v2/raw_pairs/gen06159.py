from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc())

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Amazon", "7-Eleven", "Shell"]))
    .select("id", "amount", F.rank().over(w).alias("amount_rank"))
)
