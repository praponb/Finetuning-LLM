from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")

result = (
    df_payments
    .select("id", "order_id", "amount", F.row_number().over(w).alias("row_num"))
    .orderBy(F.col("id").desc())
)
