from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") < 5212) & F.col("store_name").isNotNull())
    .select("city", "opened_date")
    .orderBy(F.col("opened_date").desc())
)
