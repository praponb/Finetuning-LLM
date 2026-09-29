from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "UK")
    .select("country")
    .subtract(df_suppliers.select("city"))
)
