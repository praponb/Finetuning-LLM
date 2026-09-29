from pyspark.sql import functions as F

result = df_subscriptions.select("status", F.initcap(F.trim("plan")).alias("plan_title"))
