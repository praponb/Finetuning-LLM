from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w2 = Window.partitionBy("status").orderBy("cost")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "status",
        "cost",
        F.count("*").over(w).alias("carrier_count"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
