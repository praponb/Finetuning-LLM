from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("discount").desc())

result = (
    df_orders
    .select("id", "store_id", "discount", F.dense_rank().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
)
