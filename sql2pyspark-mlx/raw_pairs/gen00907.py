from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("country").isin(["TH", "SG"]))
    .select("country")
    .intersect(df_sessions.select("country"))
)
