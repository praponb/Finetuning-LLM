from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")

result = (
    df_payments
    .filter(F.col("method").isin(["card", "promptpay"]))
    .select("id", "order_id", "amount", F.ntile(10).over(w).alias("quartile"))
    .orderBy(F.col("order_id").desc())
)
