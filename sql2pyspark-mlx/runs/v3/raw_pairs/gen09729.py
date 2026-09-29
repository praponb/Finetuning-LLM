from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("end_date").desc(), "id")

result = df_subscriptions.select("id", "end_date", F.row_number().over(w).alias("rn"))
