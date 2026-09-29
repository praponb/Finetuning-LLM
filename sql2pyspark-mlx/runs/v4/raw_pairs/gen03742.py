from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy(F.col("amount").desc())

result = (
    df_payments
    .select("id", "status", "method", "amount", F.rank().over(w).alias("rnk"))
)
