from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("txn_time"), "merchant").orderBy(F.col("txn_time").desc())

result = df_transactions.select("id", "txn_time", "merchant", F.rank().over(w).alias("rnk"))
