from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc())

result = (
    df_payments
    .select("id", "method", "amount", "status", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 2)
    .select("id", "method", "amount", "status")
    .orderBy(F.col("amount").desc(), "method")
)
