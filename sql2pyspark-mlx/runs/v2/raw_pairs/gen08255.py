from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .select("id", "order_id", "amount", F.row_number().over(w).alias("rn"))
)
