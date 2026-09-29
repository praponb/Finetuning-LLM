from pyspark.sql import functions as F

min_opened_date = df_stores.agg(F.min("opened_date")).first()[0]

result = (
    df_stores
    .filter(F.col("opened_date") == min_opened_date)
    .select("id", "opened_date", "region", "sqft")
    .orderBy(F.col("id").desc())
)
