from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper("email").like("%RAN%"))
    .select("country")
    .intersect(df_sessions.select("country"))
)
