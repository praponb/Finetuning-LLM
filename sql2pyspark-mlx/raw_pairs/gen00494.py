from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")

result = df_employees.select("id", "dept_id", "salary", F.ntile(4).over(w).alias("bucket"))
