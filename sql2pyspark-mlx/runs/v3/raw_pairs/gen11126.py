from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")
w2 = Window.partitionBy("status", "customer_id")

result = (
    df_orders
    .filter(F.col("store_id").isin([12, 18]))
    .select(
        "id",
        "channel",
        "discount",
        "status",
        "customer_id",
        F.row_number().over(w).alias("rn"),
        F.count("*").over(w2).alias("status_count"),
    )
)
