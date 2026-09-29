from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id")

result = (
    df_payments
    .select("id", "status", "amount", "method", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .orderBy("method")
)
