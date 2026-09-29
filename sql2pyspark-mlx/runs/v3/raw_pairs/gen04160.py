from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .filter(F.month("order_date").isin([1, 4, 11]))
    .select("id", "store_id", "order_date", F.row_number().over(w).alias("row_num"))
)
