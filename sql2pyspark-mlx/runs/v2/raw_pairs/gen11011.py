from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")

result = (
    df_payments
    .filter(F.col("order_id").isin([4, 8, 16, 17]))
    .select("id", "order_id", "amount", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "amount")
)
