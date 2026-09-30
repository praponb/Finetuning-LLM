from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")

result = (
    df_customers
    .select("id", "segment", "age", F.avg("age").over(w).alias("segment_avg_age"))
)
