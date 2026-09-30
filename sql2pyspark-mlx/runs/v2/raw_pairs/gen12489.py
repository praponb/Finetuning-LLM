from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")

result = df_stores.select("id", "opened_date", F.row_number().over(w).alias("rn"))
