from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_orders
    .select("id", "status", F.count("*").over(w).alias("status_count"))
    .orderBy(F.col("status").desc(), F.col("id").desc())
)
