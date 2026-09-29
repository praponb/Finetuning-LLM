from pyspark.sql import functions as F

result = df_subscriptions.filter((F.year("end_date") == 2024) | (F.year("end_date") >= 2024))
