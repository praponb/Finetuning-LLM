from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .filter(F.col("order_date").between("2024-10-01", "2025-04-04"))
    .select("id", "status", "discount", "channel", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 1)
    .select("id", "status", "discount", "channel")
)
