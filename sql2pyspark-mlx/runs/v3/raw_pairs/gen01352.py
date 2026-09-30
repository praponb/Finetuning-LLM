from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Bangkok", "London"]))
    .select("country")
    .unionByName(df_sessions.select("country"))
)
