from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .select("id", "customer_id", "order_date", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .orderBy("row_num")
)
