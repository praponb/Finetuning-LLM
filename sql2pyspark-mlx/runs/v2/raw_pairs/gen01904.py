from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select("id", "carrier", "cost", F.avg("cost").over(w).alias("carrier_avg_cost"))
)
