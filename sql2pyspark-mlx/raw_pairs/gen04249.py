from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_orders
    .filter(F.col("discount") >= 0.2)
    .select(
        "id",
        "status",
        "discount",
        F.min("discount").over(w).alias("status_min_discount"),
    )
)
