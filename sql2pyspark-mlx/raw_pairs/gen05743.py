from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.col("order_id").isin([8, 15, 18, 35]))
    .select("id", "order_id", "cost", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 5)
    .orderBy(F.col("cost").desc())
)
