from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date")

result = df_stores.select("id", "opened_date", F.rank().over(w).alias("opened_date_rank"))
