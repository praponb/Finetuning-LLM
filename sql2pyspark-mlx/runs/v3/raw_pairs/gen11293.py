from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .select("id", "customer_id", "discount", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .orderBy(F.col("row_num").desc(), "customer_id")
)
