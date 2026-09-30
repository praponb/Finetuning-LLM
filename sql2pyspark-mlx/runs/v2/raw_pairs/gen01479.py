from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id")

result = df_accounts.select("id", "balance", F.row_number().over(w).alias("balance_row"))
