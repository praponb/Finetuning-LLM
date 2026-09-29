from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit").desc(), "id")

result = df_customers.select("id", "credit_limit", F.ntile(4).over(w).alias("quartile"))
