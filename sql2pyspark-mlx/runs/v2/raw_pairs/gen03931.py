from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") >= "2023-10-01") | (F.col("status") == "OPEN"))
    .select("id", "opened_date", "customer_id")
)
