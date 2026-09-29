from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc(), "id")

result = df_employees.select("id", "salary", F.ntile(10).over(w).alias("quartile"))
