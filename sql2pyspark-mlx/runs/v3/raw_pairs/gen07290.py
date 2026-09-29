from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id")

result = df_customers.select("id", "segment", "age", F.ntile(3).over(w).alias("bucket"))
