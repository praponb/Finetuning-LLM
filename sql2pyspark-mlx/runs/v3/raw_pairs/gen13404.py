from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 3.1)
    .select("country")
    .unionByName(df_sessions.select("country"))
    .orderBy("country")
)
