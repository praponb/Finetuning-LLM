from pyspark.sql import functions as F

result = df_sessions.select("device", F.initcap(F.trim("device")).alias("device_title"))
