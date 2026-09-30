from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance")

result = df_accounts.select("id", "balance", F.rank().over(w).alias("rnk"))
