from pyspark.sql import functions as F

result = df_sessions.filter((F.col("device") != "tablet") & (F.col("browser") == "Firefox"))
