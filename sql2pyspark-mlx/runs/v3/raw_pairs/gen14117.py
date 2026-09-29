from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost")
w2 = Window.partitionBy("order_id")

result = (
    df_shipments
    .filter(F.col("carrier") == "FedEx")
    .select(
        "id",
        "order_id",
        "cost",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.count("*").over(w2).alias("order_id_count"),
    )
)
