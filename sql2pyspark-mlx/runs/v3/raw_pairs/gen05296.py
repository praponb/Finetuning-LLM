from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-05-01", "2023-12-01"))
    .select("id", "store_name")
    .orderBy(F.col("id").desc())
)
