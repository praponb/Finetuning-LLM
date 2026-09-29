from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc())

result = (
    df_shipments
    .select("id", "carrier", "cost", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
)
