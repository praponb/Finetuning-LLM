from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc())

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2023-06-01")
    .select("id", "status", "shipped_date", "cost", F.dense_rank().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "status", "shipped_date", "cost")
)
