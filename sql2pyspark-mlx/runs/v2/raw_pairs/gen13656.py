from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("store_name").like("Old%"))
    .select("id", "opened_date", "sqft")
    .orderBy(F.col("id").desc())
    .limit(5)
)
