from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "order_id", "shipped_date", "carrier")
)
