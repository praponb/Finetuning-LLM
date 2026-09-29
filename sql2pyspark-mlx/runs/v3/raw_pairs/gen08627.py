from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc())
w2 = Window.partitionBy("order_id")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "order_id",
        "cost",
        F.dense_rank().over(w).alias("shipped_date_dense_rank"),
        F.avg("cost").over(w2).alias("order_id_avg_cost"),
    )
    .orderBy("order_id_avg_cost")
)
