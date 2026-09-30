from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")

result = df_accounts.select("id", "opened_date", F.ntile(2).over(w).alias("bucket"))
