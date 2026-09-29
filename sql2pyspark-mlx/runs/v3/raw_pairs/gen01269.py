from pyspark.sql import functions as F

result = df_subscriptions.filter(F.col("end_date") >= "2023-10-22")
