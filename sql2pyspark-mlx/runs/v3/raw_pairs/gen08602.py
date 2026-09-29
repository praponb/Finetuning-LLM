from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") != "FROZEN") & (F.col("opened_date") > "2024-08-11"))
)
