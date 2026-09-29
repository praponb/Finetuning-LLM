from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") < "2024-02-02") | (F.col("opened_date") <= "2023-11-17"))
    .select("id", "store_name", "opened_date")
    .orderBy(F.col("opened_date").desc(), "id")
    .limit(10)
)
