from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id")

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "status", "amount", "paid_at")
)
