from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")

result = df_customers.select("id", "segment", "age", F.row_number().over(w).alias("age_row"))
