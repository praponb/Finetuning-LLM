from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["TH", "UK"]))
    .select("id", "is_active")
)
