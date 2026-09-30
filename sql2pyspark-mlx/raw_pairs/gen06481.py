from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["US", "UK", "JP"]))
    .select("country")
    .unionByName(df_sessions.filter(F.col("customer_id").isNull()).select("country"))
)
