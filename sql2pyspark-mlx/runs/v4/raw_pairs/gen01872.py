from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .orderBy("opened_date")
)
