from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") <= "2023-07-11") | F.col("store_name").isNotNull())
    .select("id", "store_name", "opened_date", "city")
)
