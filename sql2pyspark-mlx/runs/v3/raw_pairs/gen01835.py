from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc())

result = df_transactions.select("id", "merchant", "amount", F.rank().over(w).alias("rnk"))
