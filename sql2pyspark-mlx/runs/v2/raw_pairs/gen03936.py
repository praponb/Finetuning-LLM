from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select("id", "status", "shipped_date", F.ntile(10).over(w).alias("quartile"))
)
