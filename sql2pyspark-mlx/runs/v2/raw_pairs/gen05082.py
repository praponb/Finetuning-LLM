from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") > "2024-06-08") & ((F.year("opened_date") == 2023) | (F.length("store_name") > 11)))
    .select("id", "city", "opened_date", "sqft")
)
