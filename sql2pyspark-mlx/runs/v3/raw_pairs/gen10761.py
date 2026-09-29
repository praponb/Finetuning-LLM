from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc())

result = (
    df_shipments
    .select("id", "order_id", "shipped_date", F.rank().over(w).alias("rnk"))
    .orderBy("order_id")
)
