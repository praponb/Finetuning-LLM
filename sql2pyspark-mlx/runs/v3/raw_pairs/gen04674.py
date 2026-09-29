from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-08-01", "2024-05-01") & F.col("sqft").between(5792, 6880))
    .select("id", "city", "sqft")
)
