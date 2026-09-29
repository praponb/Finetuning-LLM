from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc())

result = (
    df_shipments
    .select("id", "carrier", "cost", F.rank().over(w).alias("cost_rank"))
    .orderBy("carrier")
)
