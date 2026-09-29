from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")

result = (
    df_customers
    .filter(F.col("city").isin(["Singapore", "Tokyo", "Bangkok"]))
    .select("id", "segment", "age", F.min("age").over(w).alias("segment_min_age"))
)
