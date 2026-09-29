from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc())

result = (
    df_shipments
    .select("id", "status", "shipped_date", F.dense_rank().over(w).alias("dense_rnk"))
)
