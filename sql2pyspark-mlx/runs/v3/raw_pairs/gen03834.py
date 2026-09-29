from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")

result = df_shipments.select("id", "order_id", "cost", F.row_number().over(w).alias("rn"))
