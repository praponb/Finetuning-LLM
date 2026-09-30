from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("cost").desc())

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "status",
        "cost",
        F.dense_rank().over(w).alias("dense_rnk"),
    )
)
