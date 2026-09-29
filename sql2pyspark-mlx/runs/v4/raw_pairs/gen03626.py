from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount")

result = (
    df_orders
    .select("id", "status", "discount", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .select("id", "status", "discount")
    .orderBy("status")
)
