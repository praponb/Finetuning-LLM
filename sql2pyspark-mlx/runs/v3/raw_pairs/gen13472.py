from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier")

result = (
    df_shipments
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        F.avg("cost").over(w).alias("order_id_avg_cost"),
    )
    .orderBy(F.col("order_id_avg_cost").desc(), F.col("order_id").desc())
)
