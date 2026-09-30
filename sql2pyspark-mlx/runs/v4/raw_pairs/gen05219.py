from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")

result = (
    df_payments
    .select("id", "method", "amount", F.ntile(3).over(w).alias("bucket"))
    .orderBy("id", F.col("amount").desc())
)
