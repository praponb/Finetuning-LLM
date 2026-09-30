from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .select("id", "status", "amount", "method", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .orderBy("row_num", "method")
)
