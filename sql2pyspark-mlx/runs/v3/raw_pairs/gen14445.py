from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .select("id", "customer_id", "amount", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "customer_id", "amount")
)
