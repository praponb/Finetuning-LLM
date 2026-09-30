from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date")

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "status",
        "shipped_date",
        F.dense_rank().over(w).alias("shipped_date_dense_rank"),
    )
)
