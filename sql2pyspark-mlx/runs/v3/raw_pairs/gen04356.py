from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") != "Edge") & ((F.col("device") == "desktop") | F.hour("started_at").between(10, 18)))
    .orderBy("country")
)
