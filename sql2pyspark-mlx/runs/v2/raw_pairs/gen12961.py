from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost", "id")

result = df_shipments.select("id", "status", "cost", F.row_number().over(w).alias("row_num"))
