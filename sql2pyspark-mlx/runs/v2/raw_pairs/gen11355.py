from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel")

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "channel",
        "discount",
        F.sum("discount").over(w).alias("customer_id_sum_discount"),
    )
    .orderBy(F.col("customer_id_sum_discount").desc())
)
