from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .select("id", "channel", "order_date", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") == 1)
    .select("id", "channel", "order_date")
)
