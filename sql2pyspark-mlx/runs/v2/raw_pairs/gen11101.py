from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc())

result = (
    df_subscriptions
    .filter(F.col("end_date") <= "2024-08-01")
    .select("id", "status", "end_date", "start_date", F.rank().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 2)
    .select("id", "status", "end_date", "start_date")
    .orderBy("id")
)
