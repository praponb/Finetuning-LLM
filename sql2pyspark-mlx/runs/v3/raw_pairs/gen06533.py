from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id")

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "REFUNDED"]))
    .select("id", "status", "amount", F.row_number().over(w).alias("row_num"))
)
