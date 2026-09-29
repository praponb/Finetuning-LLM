from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date", "region")
    .orderBy("region")
)
