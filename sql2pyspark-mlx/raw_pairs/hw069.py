from pyspark.sql import functions as F

result = df_subscriptions.select("id", F.add_months("start_date", 12).alias("renewal_date"))
