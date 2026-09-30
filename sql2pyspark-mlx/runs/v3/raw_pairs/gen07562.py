from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")

result = (
    df_orders
    .filter(F.col("discount") < 0.27)
    .select(
        "id",
        "channel",
        "discount",
        "customer_id",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
    .select("id", "channel", "discount", "customer_id")
)
