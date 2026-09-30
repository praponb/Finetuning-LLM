from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("country").isin(["DE", "JP", "TH"]) & F.col("browser").isin(["Edge", "Chrome"]))
    .orderBy(F.col("customer_id").desc())
)
