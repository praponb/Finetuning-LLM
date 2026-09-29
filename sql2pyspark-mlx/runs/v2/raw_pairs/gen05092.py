from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("order_date").desc())

result = (
    df_orders
    .select(
        "id",
        "channel",
        "order_date",
        "store_id",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "channel", "order_date", "store_id")
)
