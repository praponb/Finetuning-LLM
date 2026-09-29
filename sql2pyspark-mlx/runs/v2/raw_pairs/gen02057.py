from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc())

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        "carrier",
        F.sum("cost").over(w).alias("running_cost"),
        F.dense_rank().over(w2).alias("cost_dense_rank"),
    )
)
