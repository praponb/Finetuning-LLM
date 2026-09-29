from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .select(
        "id",
        "channel",
        "order_date",
        "shipping_fee",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("rn").desc())
)
