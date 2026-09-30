from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date")

result = (
    df_shipments
    .select("id", "carrier", "shipped_date", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .select("id", "carrier", "shipped_date")
)
