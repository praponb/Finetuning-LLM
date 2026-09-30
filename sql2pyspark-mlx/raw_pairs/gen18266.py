from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")

result = (
    df_payments
    .filter(F.hour("paid_at").between(7, 15))
    .select("id", "method", F.count("*").over(w).alias("method_count"))
)
