from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at")

result = df_payments.select("id", "method", "paid_at", F.rank().over(w).alias("rnk"))
