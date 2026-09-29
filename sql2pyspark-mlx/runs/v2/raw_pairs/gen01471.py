from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-03-22", "2024-03-03") & (F.col("opened_date") < "2024-04-06") & (F.col("opened_date") >= "2024-05-27"))
    .select("id", "region", "opened_date", "store_name")
    .orderBy("id")
    .limit(3)
)
