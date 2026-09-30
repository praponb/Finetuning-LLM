from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc())

result = (
    df_payments
    .filter(F.col("status") != "REFUNDED")
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "status", "amount", "method")
)
