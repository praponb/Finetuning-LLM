from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("delivered_date").desc())

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "LOST", "IN_TRANSIT"]))
    .select(
        "id",
        "status",
        "order_id",
        "delivered_date",
        F.dense_rank().over(w).alias("dense_rnk"),
    )
)
