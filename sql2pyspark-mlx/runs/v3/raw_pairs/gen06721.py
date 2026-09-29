from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id")

result = (
    df_orders
    .filter(F.col("discount") < 0.37)
    .select(
        "id",
        "status",
        "customer_id",
        "discount",
        F.sum("discount").over(w).alias("status_sum_discount"),
    )
    .orderBy(F.col("discount").desc())
)
