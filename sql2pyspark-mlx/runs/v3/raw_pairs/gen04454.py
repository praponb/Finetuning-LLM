from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "LOST"]))
    .select("id", "carrier", "cost", F.max("cost").over(w).alias("carrier_max_cost"))
)
