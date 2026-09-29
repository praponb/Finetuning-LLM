from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")

result = (
    df_orders
    .select("id", "channel", "order_date", F.row_number().over(w).alias("row_num"))
)
