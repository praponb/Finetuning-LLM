from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.orderBy("shipped_date")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "cost",
        "shipped_date",
        F.min("cost").over(w).alias("order_id_min_cost"),
        F.dense_rank().over(w2).alias("shipped_date_dense_rank"),
    )
    .orderBy(F.col("cost").desc())
)
