from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("created_at"), "key").orderBy(F.col("created_at").desc(), "id", "key")

exploded = df_carts.select("id", "created_at", F.explode("attributes").alias("key", "v"))

result = (
    exploded
    .filter(F.col("key").isin(["gift", "device"]))
    .select("id", "created_at", "key", F.row_number().over(w).alias("event_seq"))
)
