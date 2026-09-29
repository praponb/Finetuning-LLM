from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc())

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "shipped_date",
        "status",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .orderBy(F.col("id").desc())
)
