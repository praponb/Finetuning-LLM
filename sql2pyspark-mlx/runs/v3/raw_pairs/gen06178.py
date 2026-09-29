from pyspark.sql import functions as F

result = df_subscriptions.filter(F.col("start_date") <= "2023-08-07")
