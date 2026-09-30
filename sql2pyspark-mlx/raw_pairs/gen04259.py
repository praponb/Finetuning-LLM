from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")

result = df_payments.select("id", "amount", F.row_number().over(w).alias("rn"))
