from pyspark.sql import functions as F

result = df_sessions.select("id", "country", F.lower("country").alias("country_lower"))
