from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")

result = (
    df_orders
    .select("id", "status", "order_date", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "status", "order_date")
)
