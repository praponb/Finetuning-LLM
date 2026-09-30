from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id")

result = (
    df_subscriptions
    .filter(F.col("status") == "ACTIVE")
    .select("id", "status", "start_date", F.ntile(4).over(w).alias("start_date_ntile"))
)
