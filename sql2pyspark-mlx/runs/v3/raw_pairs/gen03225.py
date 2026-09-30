from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .filter(F.col("order_date").between("2025-04-01", "2025-05-20"))
    .select("id", "status", "amount", "channel", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 1)
    .select("id", "status", "amount", "channel")
)
