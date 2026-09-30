from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")

result = (
    df_payments
    .select("id", "status", "paid_at", F.ntile(10).over(w).alias("bucket"))
    .orderBy(F.col("id").desc())
)
