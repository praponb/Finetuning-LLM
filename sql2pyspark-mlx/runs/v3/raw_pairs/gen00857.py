from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select("id", "status", "plan", "end_date", F.row_number().over(w).alias("row_num"))
)
