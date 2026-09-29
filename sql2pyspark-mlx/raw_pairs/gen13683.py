from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id")

result = df_shipments.select("id", "cost", F.row_number().over(w).alias("rn"))
