from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .select("id", "status", "end_date", "plan", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 5)
    .orderBy("id")
)
