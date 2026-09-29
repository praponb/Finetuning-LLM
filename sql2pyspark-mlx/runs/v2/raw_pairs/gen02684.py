from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "mobile"]))
    .select(
        "id",
        "channel",
        "order_date",
        "discount",
        F.lag("discount", 1).over(w).alias("prev_discount"),
    )
    .orderBy(F.col("discount").desc(), "channel")
)
