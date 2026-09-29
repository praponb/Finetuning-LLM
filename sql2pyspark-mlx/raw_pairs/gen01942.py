from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("shipped_date", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("carrier", "status")

metrics = (
    df_shipments
    .select(
        "*",
        F.avg("cost").over(w).alias("cost_avg_3_prior"),
        F.avg("cost").over(w2).alias("carrier_avg_cost"),
    )
)

result = (
    metrics
    .select("id", "delivered_date", "cost", "status", "carrier_avg_cost")
    .orderBy(F.col("delivered_date").desc())
)
