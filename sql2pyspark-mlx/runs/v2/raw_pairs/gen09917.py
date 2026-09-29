from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc())

result = (
    df_payments
    .select("id", "status", "amount", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "status", "amount")
    .orderBy(F.col("id").desc(), F.col("amount").desc())
)
