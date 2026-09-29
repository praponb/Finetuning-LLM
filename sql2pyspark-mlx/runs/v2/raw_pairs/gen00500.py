from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc())

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "delivered_date",
        F.dense_rank().over(w).alias("delivered_date_dense_rank"),
    )
)
