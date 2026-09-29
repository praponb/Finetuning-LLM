from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2023-05-01")
    .select("id", "sqft")
    .orderBy(F.col("id").desc())
    .limit(1)
)
