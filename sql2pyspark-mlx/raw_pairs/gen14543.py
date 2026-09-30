from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("carrier", "status").orderBy(F.col("cost").desc_nulls_first())

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "status",
        F.ntile(5).over(w).alias("cost_bucket"),
        F.dense_rank().over(w2).alias("cost_dense_rank"),
    )
)
