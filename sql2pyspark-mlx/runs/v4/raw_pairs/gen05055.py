from pyspark.sql import functions as F

result = df_accounts.filter((F.year("opened_date") == 2023) & (F.col("status") == "OPEN"))
