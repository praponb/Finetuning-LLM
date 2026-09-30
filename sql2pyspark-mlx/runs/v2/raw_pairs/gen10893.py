from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([4, 7, 11]))
    .select("id", "status", "end_date", F.row_number().over(w).alias("end_date_row"))
)
