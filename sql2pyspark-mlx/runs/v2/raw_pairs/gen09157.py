from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~((F.col("device") != "tablet") & F.col("country").isNull() & F.col("country").isin(["US", "TH"])))
    .orderBy(F.col("browser").desc())
)
