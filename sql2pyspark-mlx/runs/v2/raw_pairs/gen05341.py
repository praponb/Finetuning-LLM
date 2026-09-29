from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .select("id", "status", "delivered_date", F.ntile(2).over(w).alias("quartile"))
)
