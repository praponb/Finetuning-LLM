from pyspark.sql import functions as F

result = df_sessions.select("id", "browser", F.upper("country").alias("country_upper"))
