from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .select("id", "shipped_date", F.ntile(10).over(w).alias("bucket"))
    .orderBy("shipped_date")
)
