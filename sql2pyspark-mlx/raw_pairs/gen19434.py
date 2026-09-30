from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") != "TH") & F.hour("started_at").between(12, 18))
)
